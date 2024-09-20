/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "sasl_aws_server_protocol.h"

#include <boost/algorithm/string/finder.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <fmt/format.h>
#include <iostream>

#include "mongo/base/data_range_cursor.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/platform/mutex.h"
#include "mongo/platform/random.h"
#include "mongo/util/base64.h"
#include "mongo/util/str.h"

namespace mongo {

namespace awsIam {
SaslAWSGlobalParams saslAWSGlobalParams;
}  // namespace awsIam

namespace {
using namespace fmt::literals;

// Secure Random for SASL IAM Nonce generation
stdx::mutex saslAWSServerMutex;
SecureRandom saslAWSServerGen;

std::array<StringData, 10> allowedHeaders = {"content-length"_sd,
                                             "content-type"_sd,
                                             "host"_sd,
                                             "x-amz-date"_sd,
                                             "x-amz-security-token"_sd,
                                             awsIam::kMongoGS2CBHeader,
                                             "x-mongodb-optional-data"_sd,
                                             awsIam::kMongoServerNonceHeader};

constexpr auto kArnSegment = "arn"_sd;
constexpr auto kStsSegment = "sts"_sd;
constexpr auto kIamSegment = "iam"_sd;

constexpr auto kAssumedRole = "assumed-role"_sd;
constexpr auto kUser = "user"_sd;

constexpr auto signedHeadersStr = "SignedHeaders="_sd;
constexpr auto credentialStr = "Credential="_sd;

template <typename It>
StringData stringDataFromRange(It first, It last) {
    if (auto d = std::distance(first, last))
        return StringData{&*first, static_cast<size_t>(d)};
    return {};
}

/**
 * Validate the SignedHeaders list of the Authorization header of AWS Sig V4
 * See https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
 *
 * SignedHeaders=(lower_case_header)(;lower_case_header)+,
 *
 * All headers are lower case and delimited by semicolons.
 */
void validateSignedHeaders(StringData authHeader) {
    size_t pos = authHeader.find(signedHeadersStr);
    uassert(51293, "SignedHeaders missing from Authorization Header", pos != std::string::npos);

    size_t trailingComma = authHeader.find(',', pos);
    uassert(51292, "SignedHeaders missing trailing comma", trailingComma != std::string::npos);

    StringData signedHeaders = authHeader.substr(pos + signedHeadersStr.size(),
                                                 trailingComma - (pos + signedHeadersStr.size()));

    size_t headerIndex = 0;
    bool hasMongoDBGS2CbFlag = false;
    bool hasMongoDBServerNonce = false;

    for (auto partIt = boost::split_iterator<StringData::const_iterator>(
             signedHeaders.begin(), signedHeaders.end(), boost::token_finder([](char c) {
                 return c == ';';
             }));
         partIt != boost::split_iterator<StringData::const_iterator>();
         ++partIt) {
        StringData header = stringDataFromRange(partIt->begin(), partIt->end());
        uassert(51291, "Too many headers", headerIndex < allowedHeaders.size());

        if (header == awsIam::kMongoGS2CBHeader) {
            hasMongoDBGS2CbFlag = true;
        } else if (header == awsIam::kMongoServerNonceHeader) {
            hasMongoDBServerNonce = true;
        }

        if (header == allowedHeaders[headerIndex]) {
            // The header is expected, advance one and continue
            headerIndex++;
        } else {
            auto origHeaderIndex = headerIndex;

            // The header is not expected, advance one and check again until we find a match
            // or we run out of allowed headers
            for (; headerIndex < (allowedHeaders.size()) && header != allowedHeaders[headerIndex];
                 headerIndex++) {
            }

            uassert(
                51290,
                str::stream() << "Did not find an expected header in its expected position. "
                                 "Only certain headers are permitted "
                                 "and headers must be sorted lexographically. Expected header: '"
                              << allowedHeaders[origHeaderIndex] << "', Actual header: '" << header
                              << "'",
                headerIndex < allowedHeaders.size());
        }
    }

    uassert(51289, "The x-mongodb-gs2-cb-flag header is missing", hasMongoDBGS2CbFlag);
    uassert(51288, "The x-mongodb-server-nonce header is missing", hasMongoDBServerNonce);
}

std::string extractAwsAccountId(StringData authHeader) {
    size_t pos = authHeader.find(credentialStr);
    uassert(51742, "Credential missing from Authorization Header", pos != std::string::npos);

    size_t trailingSlash = authHeader.find('/', pos);
    uassert(51743, "Credential missing trailing slash", trailingSlash != std::string::npos);

    return authHeader.substr(pos + credentialStr.size(), trailingSlash - pos - credentialStr.size())
        .toString();
}

}  // namespace

std::array<char, 32> awsIam::generateServerNonce() {

    std::array<char, awsIam::kServerFirstNoncePieceLength> ret;

    {
        stdx::lock_guard<Latch> lk(saslAWSServerMutex);
        saslAWSServerGen.fill(&ret, ret.size());
    }

    return ret;
}

std::string awsIam::generateServerFirst(StringData clientFirstBase64,
                                        std::vector<char>* serverNonce,
                                        char* cbFlag) {
    auto clientFirst = awsIam::convertFromByteString<AwsClientFirst>(clientFirstBase64);

    uassert(51285,
            "Nonce must be 32 bytes",
            clientFirst.getNonce().length() == awsIam::kClientFirstNonceLength);
    uassert(51284,
            "Channel Binding Prefix must not be 'p'",
            clientFirst.getGs2_cb_flag() == 'n' || clientFirst.getGs2_cb_flag() == 'y');

    *cbFlag = clientFirst.getGs2_cb_flag();

    auto serverNoncePiece = generateServerNonce();

    AwsServerFirst first;

    serverNonce->reserve(awsIam::kServerFirstNonceLength);

    auto cdr = clientFirst.getNonce();
    std::copy(cdr.data(), cdr.data() + cdr.length(), std::back_inserter(*serverNonce));
    std::copy(serverNoncePiece.begin(), serverNoncePiece.end(), std::back_inserter(*serverNonce));

    first.setServerNonce(*serverNonce);
    first.setStsHost(saslAWSGlobalParams.awsSTSHost);

    return awsIam::convertToByteString(first);
}

std::tuple<std::vector<std::string>, std::string> awsIam::parseClientSecond(
    StringData clientSecondStr,
    const std::vector<char>& serverNonce,
    char cbFlag,
    std::string* awsAccountId) {
    auto clientSecond = awsIam::convertFromByteString<AwsClientSecond>(clientSecondStr);

    validateSignedHeaders(clientSecond.getAuthHeader());
    *awsAccountId = extractAwsAccountId(clientSecond.getAuthHeader());

    /* Retrieve arguments */
    constexpr auto requestBody = "Action=GetCallerIdentity&Version=2011-06-15"_sd;

    std::vector<std::string> headers;
    static_assert(requestBody.size() == 43);
    headers.push_back("Content-Length:43");
    headers.push_back("Content-Type:application/x-www-form-urlencoded");
    headers.push_back("Host:" + saslAWSGlobalParams.awsSTSHost);
    headers.push_back("X-Amz-Date:" + clientSecond.getXAmzDate());

    if (clientSecond.getXAmzSecurityToken()) {
        headers.push_back("X-Amz-Security-Token:" + clientSecond.getXAmzSecurityToken().value());
    }

    headers.push_back(awsIam::kMongoServerNonceHeader + ":" +
                      base64::encode(StringData(serverNonce.data(), serverNonce.size())));
    headers.push_back(str::stream() << awsIam::kMongoGS2CBHeader << ':' << cbFlag);

    headers.push_back("Authorization:" + clientSecond.getAuthHeader());

    return {headers, requestBody.toString()};
}

std::string awsIam::getArn(StringData request) {
    std::stringstream istr(request.toString());

    boost::property_tree::ptree tree;

    boost::property_tree::read_xml(istr, tree);

    auto arnStr =
        tree.get_optional<std::string>("GetCallerIdentityResponse.GetCallerIdentityResult.Arn");

    uassert(51283, "Failed to parse GetCallerIdentityResponse", arnStr);

    return arnStr.value();
}

std::string awsIam::makeSimplifiedArn(StringData arn) {
    using Range = boost::iterator_range<const char*>;
    auto parsePathComponents = [](Range segment) {
        std::list<Range> components;
        boost::algorithm::split(components, segment, [](auto c) { return c == '/'; });
        return components;
    };

    auto parseSegments = [](Range arn) {
        std::list<Range> segments;
        boost::algorithm::split(segments, arn, [](auto c) { return c == ':'; });
        return segments;
    };

    auto coerceToStringData = [](Range range) { return StringData(range.begin(), range.size()); };

    const auto segments = parseSegments({arn.data(), arn.data() + arn.size()});

    uassert(5479900, "ARNs must consist of at least 6 segments", segments.size() >= 6);

    auto segmentIt = segments.begin();
    const auto arnSegment = coerceToStringData(*segmentIt++);
    const auto partitionSegment = coerceToStringData(*segmentIt++);
    const auto serviceSegment = coerceToStringData(*segmentIt++);
    const auto regionSegment = coerceToStringData(*segmentIt++);
    const auto accountIdSegment = coerceToStringData(*segmentIt++);
    const auto resourceSegment = *segmentIt++;  // Skip coercing since we need to break it apart.

    uassert(5479901, "ARNs must start with \"arn\"", arnSegment == kArnSegment);

    if (serviceSegment == kIamSegment) {
        // For IAM ARNs, only user resources are permitted
        const auto resourcePathComponents = parsePathComponents(resourceSegment);
        const auto resourceTypeSegment = coerceToStringData(resourcePathComponents.front());
        uassert(51280, "ARN has unacceptable resource-type for IAM", resourceTypeSegment == kUser);

        return arn.toString();
    } else if (serviceSegment == kStsSegment) {
        auto resourcePathComponents = parsePathComponents(resourceSegment);
        const auto resourceTypeSegment = coerceToStringData(resourcePathComponents.front());
        resourcePathComponents.pop_front();
        uassert(51279,
                "ARN has unacceptable resource-type for STS",
                resourceTypeSegment == kAssumedRole);

        if (resourcePathComponents.empty()) {
            uassert(5479903, "ARN lacks resource-id", segmentIt != segments.end());
            resourcePathComponents = parsePathComponents(*segmentIt++);
        }

        uassert(51278,
                "ARN must have at list two path components in its resource-id",
                resourcePathComponents.size() >= 2);
        StringData trimmedResourceId = stringDataFromRange(
            resourcePathComponents.front().begin(), resourcePathComponents.back().begin() - 1);

        // Build a simplified ARN.
        return "{}:{}:{}:{}:{}:{}/{}/*"_format(arnSegment,
                                               partitionSegment,
                                               serviceSegment,
                                               regionSegment,
                                               accountIdSegment,
                                               resourceTypeSegment,
                                               trimmedResourceId);
    } else {
        uasserted(5479902, "ARN has unacceptable service");
    }

    MONGO_UNREACHABLE;
}

}  // namespace mongo
