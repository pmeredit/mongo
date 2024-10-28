/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_api_operator.h"

#include <algorithm>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <cstdio>
#include <memory>

#include "mongo/base/error_codes.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/http_client.h"
#include "streams/exec/context.h"
#include "streams/exec/feature_flag.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/operator.h"
#include "streams/exec/rate_limiter.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

ExternalApiOperator::ExternalApiOperator(Context* context, ExternalApiOperator::Options options)
    : Operator(context, 1, 1),
      _options(std::move(options)),
      _rateLimitPerSec{getRateLimitPerSec(*context->featureFlags)},
      _rateLimiter{_rateLimitPerSec, &_options.timer},
      _cidrDenyList{parseCidrDenyList()} {}

std::vector<CIDR> ExternalApiOperator::parseCidrDenyList() {
    auto denyList =
        _context->featureFlags->getFeatureFlagValue(FeatureFlags::kCidrDenyList).getVectorString();
    if (!denyList) {
        return {};
    }
    std::vector<CIDR> cidrDenyList{};
    cidrDenyList.reserve(denyList->size());
    for (const auto& v : *denyList) {
        auto cidrWithStatus = CIDR::parse(v);
        tassert(9503500,
                str::stream() << "Deny list value is not a CIDR: '" << v << "'",
                cidrWithStatus.isOK());
        auto cidr = cidrWithStatus.getValue();
        cidrDenyList.emplace_back(cidr);
    }
    return cidrDenyList;
}

void ExternalApiOperator::initializeHTTPClient() {
    auto client = mongo::HttpClient::createWithFirewall(_cidrDenyList);
    client->setConnectTimeout(_options.connectionTimeoutSecs);
    client->setTimeout(_options.requestTimeoutSecs);
    client->allowInsecureHTTP(getTestCommandsEnabled());
    _options.httpClient = std::move(client);
}

void ExternalApiOperator::doStart() {
    // TODO(SERVER-95031): evaluate and set connection headers
    if (!_options.httpClient) {
        initializeHTTPClient();
    }
}

const ExternalApiOperator::Options& ExternalApiOperator::getOptions() {
    return _options;
}

void ExternalApiOperator::doOnDataMsg(int32_t inputIdx,
                                      StreamDataMsg dataMsg,
                                      boost::optional<StreamControlMsg> controlMsg) {
    auto outputMsgDocsSize = std::min(int(dataMsg.docs.size()), int(kDataMsgMaxDocSize));

    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(outputMsgDocsSize);
    int32_t curDataMsgByteSize{0};

    int64_t numDlqDocs{0};
    int64_t numDlqBytes{0};

    // If the rate limit per second feature flag is updated, update rate limiter accordingly
    if (auto newRateLimitPerSec = getRateLimitPerSec(*_context->featureFlags);
        newRateLimitPerSec != _rateLimitPerSec) {
        _rateLimitPerSec = newRateLimitPerSec;
        _rateLimiter.setTokensRefilledPerSec(newRateLimitPerSec);
        _rateLimiter.setCapacity(newRateLimitPerSec);
    }

    if (_context->featureFlags->isOverridden(FeatureFlags::kCidrDenyList)) {
        auto updatedDenyList = parseCidrDenyList();
        if (updatedDenyList != _cidrDenyList) {
            _cidrDenyList = updatedDenyList;
            initializeHTTPClient();
        }
    }

    for (auto& streamDoc : dataMsg.docs) {
        if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
            curDataMsgByteSize = 0;
            outputMsg = StreamDataMsg{};
            outputMsg.docs.reserve(outputMsgDocsSize);
        }

        auto& inputDoc = streamDoc.doc;
        boost::optional<mongo::HttpClient::HttpReply> httpResponse;

        try {
            httpResponse = doRequest(inputDoc);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            // TODO(SERVER-95033): add more onError handlers in response to network errors, and
            // certain http status codes
            numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
                _context->streamMetaFieldName, streamDoc, doGetName(), std::move(error)));
            numDlqDocs++;
            continue;
        }

        tassert(9502901, "Expected HTTP response to be set", httpResponse);
        try {
            // readAndAdvance can fail to parse response body and throw an exception.
            auto rawResponse = httpResponse->body.getCursor().readAndAdvance<StringData>();
            auto bufferLength = rawResponse.size();
            auto responseView =
                bsoncxx::stdx::string_view{std::move(rawResponse.data()), bufferLength};
            auto apiResponse = fromBsoncxxDocument(bsoncxx::from_json(responseView));

            // TODO(SERVER-95033): handle plaintext response.

            auto joinedDoc = makeDocumentWithAPIResponse(inputDoc, Value(std::move(apiResponse)));
            curDataMsgByteSize += joinedDoc.getApproximateSize();

            streamDoc.doc = std::move(joinedDoc);
            outputMsg.docs.emplace_back(std::move(streamDoc));
        } catch (const DBException& e) {
            // TODO(SERVER-95033): add more error handling methods in response to unexpected
            // response types, deserialization errors, etc.
            std::string error = str::stream()
                << "Failed to parse response body in " << getName() << " with error: " << e.what();
            numDlqBytes +=
                _context->dlq->addMessage(toDeadLetterQueueMsg(_context->streamMetaFieldName,
                                                               std::move(streamDoc),
                                                               doGetName(),
                                                               std::move(error)));
            numDlqDocs++;
            continue;
        }
    }

    incOperatorStats({.numDlqDocs = numDlqDocs, .numDlqBytes = numDlqBytes});
    // TODO(SERVER-95602): expose ingress/egress metrics
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void ExternalApiOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // Let the control message flow to the next operator.
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

boost::optional<mongo::HttpClient::HttpReply> ExternalApiOperator::doRequest(
    const StreamDocument& streamDoc) {

    auto headers = evaluateHeaders(streamDoc.doc);

    std::string rawDoc;
    ConstDataRange httpPayload = {nullptr, 0};
    switch (_options.requestType) {
        case HttpClient::HttpMethod::kPOST:
        case HttpClient::HttpMethod::kPUT:
        case HttpClient::HttpMethod::kPATCH: {
            rawDoc = tojson(streamDoc.doc.toBson(), mongo::JsonStringFormat::ExtendedRelaxedV2_0_0);
            httpPayload = ConstDataRange(rawDoc, rawDoc.size());
            headers.emplace_back("Content-Type: application/json");
            break;
        }
        default:
            // Payload is not used by other HTTP Methods
            break;
    }

    tassert(9502900,
            "Expected http client to exist before trying to make requests.",
            _options.httpClient);
    _options.httpClient->setHeaders(headers);

    // Wait for throttle delay before performing request
    auto throttleDelay = _rateLimiter.consume();
    if (throttleDelay > Microseconds(0)) {
        _options.throttleFn(throttleDelay);
        tassert(
            9503700, "Expected a zero throttle delay", _rateLimiter.consume() == Microseconds(0));
    }

    auto response = _options.httpClient->request(
        _options.requestType,
        (_options.urlPathExpr) ? evaluateFullUrl(streamDoc.doc) : _options.url,
        httpPayload);

    uassert(ErrorCodes::OperationFailed,
            str::stream() << "Unexpected http status code from server: " << response.code,
            response.code >= 200 && response.code < 300);

    return response;
}

std::string ExternalApiOperator::evaluateFullUrl(const mongo::Document& doc) {
    uassert(
        ErrorCodes::InternalError,
        str::stream() << "evaluateFullURL should only be called if _options.path is an not nullptr",
        _options.urlPathExpr);
    Value pathField = _options.urlPathExpr->evaluate(doc, &_context->expCtx->variables);
    if (!pathField.missing() && pathField.getType() == String) {
        return _options.url + pathField.getStringData();
    }
    // TODO(SERVER-95031): handle if path is missing or evaluates to non string and DLQ if the
    // expression evaluate throws an exception
    return "";
}

std::vector<std::string> ExternalApiOperator::evaluateHeaders(const mongo::Document& doc) {
    // TODO(SERVER-95031): evaluate the operator headers with the current stream document and
    // combine with the connection headers.
    return _options.connectionHeaders;
}

mongo::Document ExternalApiOperator::makeDocumentWithAPIResponse(mongo::Document inputDoc,
                                                                 mongo::Value apiResponse) {
    MutableDocument output(std::move(inputDoc));
    output.setNestedField(_options.as, std::move(apiResponse));
    return output.freeze();
}

int64_t getRateLimitPerSec(boost::optional<StreamProcessorFeatureFlags> featureFlags) {
    tassert(9503701, "Feature flags should be set", featureFlags);
    auto val =
        featureFlags->getFeatureFlagValue(FeatureFlags::kExternalAPIRateLimitPerSecond).getInt();

    return *val;
}

}  // namespace streams
//
