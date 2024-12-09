/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/https_operator.h"

#include "streams/exec/constants.h"
#include <algorithm>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <cstdio>
#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/urlapi.h>
#include <memory>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/json.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/http_client.h"
#include "mongo/util/overloaded_visitor.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/feature_flag.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/operator.h"
#include "streams/exec/rate_limiter.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace {
std::string getHeaderStr(const std::string& name, const std::string& val) {
    return fmt::format("{}: {}", name, val);
}

std::string getQueryParamsStr(const std::string& key, const std::string& value) {
    return fmt::format("{}={}", key, value);
}

std::string urlEncode(const std::string& str) {
    char* encoded = curl_easy_escape(nullptr, str.c_str(), str.length());
    tassert(9617500, "Failed to url-encode string.", encoded != nullptr);
    ScopeGuard guard([&] { curl_free(encoded); });

    return std::string{encoded};
}

HttpMethodEnum httpMethodClientToIDLType(HttpClient::HttpMethod method) {
    switch (method) {
        case mongo::HttpClient::HttpMethod::kGET:
            return HttpMethodEnum::MethodGet;
        case mongo::HttpClient::HttpMethod::kPOST:
            return HttpMethodEnum::MethodPost;
        case mongo::HttpClient::HttpMethod::kPUT:
            return HttpMethodEnum::MethodPut;
        case mongo::HttpClient::HttpMethod::kPATCH:
            return HttpMethodEnum::MethodPatch;
        case mongo::HttpClient::HttpMethod::kDELETE:
            return HttpMethodEnum::MethodDelete;
        default:
            tasserted(ErrorCodes::InternalError, "Unknown method");
    }
}

std::string urlEncodeAndJoinQueryParams(const std::vector<std::string>& params) {
    std::vector<std::string> encoded;
    encoded.reserve(params.size());

    for (const auto& param : params) {
        size_t splitIdx = param.find('=');
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                str::stream()
                    << "Query parameters defined in " << kHttpsStageName
                    << " must have key-value pairs separated with a '=' character non-empty keys.",
                splitIdx != std::string::npos && splitIdx > 0);
        std::string key = urlEncode(param.substr(0, splitIdx));
        std::string value = urlEncode(param.substr(splitIdx + 1));
        std::string kvString;
        str::joinStringDelim({std::move(key), std::move(value)}, &kvString, '=');
        encoded.push_back(std::move(kvString));
    }

    std::string out;
    str::joinStringDelim(encoded, &out, '&');
    return out;
}

void validateQueryParam(const std::string& param) {
    std::vector<std::string> keyAndValue;
    str::splitStringDelim(param, &keyAndValue, '=');
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Query parameters defined in " << kHttpsStageName
                          << " must separate the key and value with a '=' character.",
            keyAndValue.size() == 2);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Query parameters defined in " << kHttpsStageName
                          << " must have a non-empty key.",
            !keyAndValue.at(0).empty());
}

}  // namespace

using namespace mongo;

void HttpsOperator::registerMetrics(MetricManager* metricManager) {
    _throttleDurationCounter = metricManager->registerCounter(
        "rest_operator_throttle_duration_micros",
        "Time slept by the rest operator to not exceed the rate limiter in microseconds",
        getDefaultMetricLabels(_context));
}

HttpsOperator::HttpsOperator(Context* context, HttpsOperator::Options options)
    : Operator(context, 1, 1),
      _options(std::move(options)),
      _rateLimitPerSec{getRateLimitPerSec(*context->featureFlags)},
      _rateLimiter{_rateLimitPerSec, &_options.timer},
      _cidrDenyList{parseCidrDenyList()} {
    tassert(
        9688098, str::stream() << "Insufficient libcurl version.", LIBCURL_VERSION_NUM >= 0x074e00);

    _stats.connectionType = ConnectionTypeEnum::HTTPS;
    parseBaseUrl();
    validateOptions();

    if (!_options.pathExpr && _options.queryParams.empty()) {
        _staticEncodedUrl = makeUrlString({}, {});
    }
}

std::vector<CIDR> HttpsOperator::parseCidrDenyList() {
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

void HttpsOperator::initializeHTTPClient() {
    auto client = mongo::HttpClient::createWithFirewall(_cidrDenyList);
    client->setConnectTimeout(_options.connectionTimeoutSecs);
    client->setTimeout(_options.requestTimeoutSecs);
    client->allowInsecureHTTP(getTestCommandsEnabled());
    _options.httpClient = std::move(client);
}

void HttpsOperator::doStart() {
    if (!_options.httpClient) {
        initializeHTTPClient();
    }
}

const HttpsOperator::Options& HttpsOperator::getOptions() {
    return _options;
}

void HttpsOperator::doOnDataMsg(int32_t inputIdx,
                                StreamDataMsg dataMsg,
                                boost::optional<StreamControlMsg> controlMsg) {
    auto outputMsgDocsSize = std::min(int(dataMsg.docs.size()), int(kDataMsgMaxDocSize));

    StreamDataMsg outputMsg;
    outputMsg.docs.reserve(outputMsgDocsSize);
    int32_t curDataMsgByteSize{0};

    int64_t numInputBytes{0};
    int64_t numOutputBytes{0};
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

        auto result = processStreamDoc(&streamDoc);
        if (result.addDocToOutputMsg) {
            curDataMsgByteSize += result.dataMsgBytes;
            outputMsg.docs.emplace_back(std::move(streamDoc));
        }
        numInputBytes += result.numInputBytes;
        numOutputBytes += result.numOutputBytes;
        numDlqDocs += result.numDlqDocs;
        numDlqBytes += result.numDlqBytes;
    }

    incOperatorStats({.numInputBytes = numInputBytes,
                      .numOutputBytes = numOutputBytes,
                      .numDlqDocs = numDlqDocs,
                      .numDlqBytes = numDlqBytes,
                      .timeSpent = dataMsg.creationTimer->elapsed()});
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void HttpsOperator::writeToStreamMeta(StreamDocument* streamDoc,
                                      const std::string& requestUrl,
                                      const mongo::HttpClient::HttpReply httpResponse,
                                      double responseTimeMs) {
    StreamMetaHttps streamMetaHttps;
    streamMetaHttps.setUrl(requestUrl);
    streamMetaHttps.setMethod(httpMethodClientToIDLType(_options.method));
    streamMetaHttps.setHttpStatusCode(httpResponse.code);
    streamMetaHttps.setResponseTimeMs(responseTimeMs);

    streamDoc->streamMeta.setHttps(std::move(streamMetaHttps));

    if (!_context->projectStreamMetaPriorToSinkStage) {
        return;
    }

    tassert(9503302, "Expected streamMetaFieldName to be set", _context->streamMetaFieldName);

    auto newStreamMeta = updateStreamMeta(streamDoc->doc.getField(*_context->streamMetaFieldName),
                                          streamDoc->streamMeta);

    MutableDocument mutableDoc(std::move(streamDoc->doc));
    mutableDoc.setField(*_context->streamMetaFieldName, Value(std::move(newStreamMeta)));
    streamDoc->doc = mutableDoc.freeze();
}

void HttpsOperator::writeToDLQ(StreamDocument* streamDoc,
                               const mongo::Document& payloadDoc,
                               const std::string& errorMsg,
                               ProcessResult& result) {
    const std::string dlqErrorMsg = str::stream()
        << "Failed to process input document in " << kHttpsStageName << " with error: " << errorMsg;
    result.numDlqBytes +=
        _context->dlq->addMessage(toDeadLetterQueueMsg(_context->streamMetaFieldName,
                                                       streamDoc->streamMeta,
                                                       payloadDoc,
                                                       doGetName(),
                                                       std::move(dlqErrorMsg)));
    result.numDlqDocs++;
}

void HttpsOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // Let the control message flow to the next operator.
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

HttpsOperator::ProcessResult HttpsOperator::processStreamDoc(StreamDocument* streamDoc) {
    ProcessResult processResult{};

    boost::optional<Document> result;
    if (_options.payloadPipeline) {
        _options.payloadPipeline->addDocument(streamDoc->doc);
        try {
            result = _options.payloadPipeline->getNext();
        } catch (const DBException& e) {
            writeToDLQ(streamDoc, streamDoc->doc, e.what(), processResult);
            return processResult;
        }
        tassert(9503400, "Expected result doc to exist", result);
    }

    const mongo::Document& payloadDoc = result ? *result : streamDoc->doc;
    auto& inputDoc = streamDoc->doc;
    boost::optional<mongo::HttpClient::HttpReply> httpResponse;

    std::string requestUrl;
    std::vector<std::string> headers;
    try {
        requestUrl = (_options.pathExpr || _options.queryParams.size() > 0)
            ? evaluateFullUrl(streamDoc->doc)
            : _staticEncodedUrl;
        headers = evaluateHeaders(inputDoc);
    } catch (const DBException& e) {
        writeToDLQ(streamDoc, payloadDoc, e.what(), processResult);
        return processResult;
    }

    std::string rawDoc;
    switch (_options.method) {
        case HttpClient::HttpMethod::kPOST:
        case HttpClient::HttpMethod::kPUT:
        case HttpClient::HttpMethod::kPATCH: {
            rawDoc = tojson(payloadDoc.toBson(), mongo::JsonStringFormat::ExtendedRelaxedV2_0_0);
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
    _options.httpClient->setHeaders(std::move(headers));

    ConstDataRange data{nullptr, 0};
    if (!rawDoc.empty()) {
        data = ConstDataRange{rawDoc, static_cast<long>(rawDoc.size())};
        processResult.numOutputBytes += rawDoc.size();
    }

    // Wait for throttle delay before performing request
    auto throttleDelay = _rateLimiter.consume();
    if (throttleDelay > Microseconds(0)) {
        _options.throttleFn(throttleDelay);
        _throttleDurationCounter->increment(durationCount<Microseconds>(throttleDelay));
        tassert(
            9503700, "Expected a zero throttle delay", _rateLimiter.consume() == Microseconds(0));
    }

    std::function<bool(const std::string, const std::exception&)> onErrorHandleException =
        [&](const std::string& msgPrefix, const std::exception& e) {
            switch (_options.onError) {
                case mongo::OnErrorEnum::DLQ: {
                    writeToDLQ(streamDoc,
                               payloadDoc,
                               fmt::format(
                                   "{} in {} with error: {}", msgPrefix, kHttpsStageName, e.what()),
                               processResult);
                    return true;
                }
                case mongo::OnErrorEnum::Ignore: {
                    return false;
                }
                case mongo::OnErrorEnum::Fail: {
                    throw;
                }
                default: {
                    tasserted(9683601, "Invalid onError option set");
                    break;
                }
            }
        };

    double responseTimeMs{0};
    StringData rawResponse;
    try {
        Timer timer{};

        if (mongo::getTestCommandsEnabled()) {
            LOGV2_INFO(9688099,
                       "Making https request.",
                       "context"_attr = _context,
                       "url"_attr = requestUrl);
        }
        httpResponse = _options.httpClient->request(_options.method, requestUrl, data);
        tassert(9502901, "Expected HTTP response to be set", httpResponse);
        rawResponse = httpResponse->body.getCursor().readAndAdvance<StringData>();

        responseTimeMs = durationCount<Milliseconds>(timer.elapsed());
        processResult.numInputBytes += rawResponse.size();
        uassert(ErrorCodes::OperationFailed,
                fmt::format("Received error response from destination server. Status: {}, Body: {}",
                            httpResponse->code,
                            rawResponse),
                httpResponse->code >= 200 && httpResponse->code < 300);
    } catch (const DBException& e) {
        tryLog(9604800, [&](int logID) {
            LOGV2_INFO(logID,
                       "Error occurred while performing request in HttpsOperator",
                       "context"_attr = _context,
                       "error"_attr = e.what());
        });
        if (onErrorHandleException("Request failure", e)) {
            return processResult;
        }
    }

    mongo::Value responseAsValue;
    if (!rawResponse.empty()) {
        boost::optional<std::string> contentType;
        auto rawHeaders = httpResponse->header.getCursor().readAndAdvance<StringData>();
        if (!rawHeaders.empty()) {
            contentType = parseContentTypeFromHeaders(rawHeaders);
        }

        if (contentType && *contentType == kTextPlain) {
            responseAsValue = Value(rawResponse.toString());
        } else if ((contentType && *contentType == kApplicationJson) || !contentType) {
            try {
                auto responseView =
                    bsoncxx::stdx::string_view{std::move(rawResponse.data()), rawResponse.size()};
                auto responseAsBson = fromBsoncxxDocument(bsoncxx::from_json(responseView));
                responseAsValue = Value(std::move(responseAsBson));
            } catch (const bsoncxx::exception& e) {
                tryLog(9604801, [&](int logID) {
                    LOGV2_INFO(logID,
                               "Error occured while reading response in HttpsOperator",
                               "context"_attr = _context,
                               "error"_attr = e.what());
                });
                if (onErrorHandleException("Failed to parse response body", e)) {
                    return processResult;
                }
            }
        } else {
            tryLog(9683603, [&](int logID) {
                LOGV2_INFO(logID,
                           "Unsupported response content-type",
                           "context"_attr = _context,
                           "content_type"_attr = contentType);
            });

            switch (_options.onError) {
                case mongo::OnErrorEnum::DLQ: {
                    writeToDLQ(streamDoc,
                               payloadDoc,
                               fmt::format("Unsupported response content-type ({}) in {} only {} "
                                           "and {} are supported",
                                           *contentType,
                                           kHttpsStageName,
                                           kApplicationJson,
                                           kTextPlain),
                               processResult);
                    return processResult;
                }
                case mongo::OnErrorEnum::Ignore: {
                    break;
                }
                case mongo::OnErrorEnum::Fail: {
                    uasserted(9683604,
                              fmt::format("Unsupported response content-type ({}) in {} only {} "
                                          "and {} are supported",
                                          *contentType,
                                          kHttpsStageName,
                                          kApplicationJson,
                                          kTextPlain));
                }
                default: {
                    tasserted(9503303, "Invalid onError option set");
                    break;
                }
            }
        }
    }

    writeToStreamMeta(streamDoc, requestUrl, std::move(*httpResponse), responseTimeMs);
    if (!responseAsValue.missing()) {
        auto joinedDoc = makeDocumentWithAPIResponse(inputDoc, std::move(responseAsValue));
        processResult.dataMsgBytes = joinedDoc.getApproximateSize();
        streamDoc->doc = std::move(joinedDoc);
    } else {
        processResult.dataMsgBytes = streamDoc->doc.getApproximateSize();
    }
    processResult.addDocToOutputMsg = true;

    return processResult;
}

std::string HttpsOperator::evaluateFullUrl(const mongo::Document& doc) {
    std::vector<std::string> queryParams;
    std::string path;
    if (_options.pathExpr) {
        path = evaluatePath(doc);
    }

    if (!_options.queryParams.empty()) {
        queryParams = evaluateQueryParams(doc);
    }

    return makeUrlString(std::move(path), std::move(queryParams));
}

std::string HttpsOperator::evaluatePath(const mongo::Document& doc) {
    Value pathValue = _options.pathExpr->evaluate(doc, &_context->expCtx->variables);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            fmt::format("Expected {}.{} to evaluate to a string.",
                        kHttpsStageName,
                        HttpsOptions::kPathFieldName),
            pathValue.getType() == mongo::BSONType::String);
    return pathValue.getString();
}

std::vector<std::string> HttpsOperator::evaluateHeaders(const mongo::Document& doc) {
    std::vector<std::string> headers{};
    headers.reserve(_options.operatorHeaders.size() + _options.connectionHeaders.size());
    for (const auto& header : _options.connectionHeaders) {
        headers.emplace_back(header);
    }
    for (const auto& kv : _options.operatorHeaders) {
        // Note: the planner parses out expressions that are represented using strings so if
        // operatorHeaders contains a string value, it is guaranteed to be a static string.
        std::visit(
            OverloadedVisitor{
                [&](const std::string& str) { headers.emplace_back(getHeaderStr(kv.first, str)); },
                [&](const boost::intrusive_ptr<mongo::Expression>& expr) {
                    auto headerVal = expr->evaluate(doc, &_context->expCtx->variables);
                    uassert(ErrorCodes::StreamProcessorInvalidOptions,
                            fmt::format("Expected {}.{} values to evaluate to a string.",
                                        kHttpsStageName,
                                        HttpsOptions::kHeadersFieldName),
                            headerVal.getType() == mongo::BSONType::String);
                    headers.emplace_back(getHeaderStr(kv.first, headerVal.getString()));
                }},
            kv.second);
    }
    return headers;
}

std::vector<std::string> HttpsOperator::evaluateQueryParams(const mongo::Document& doc) {
    std::vector<std::string> keyValues;
    if (_options.queryParams.empty()) {
        return keyValues;
    }

    keyValues.reserve(_options.queryParams.size());
    for (const auto& kv : _options.queryParams) {
        // Note: the planner parses out expressions that are represented using strings so if
        // operatorHeaders has a string value, it is guaranteed to be a static string.
        const std::string& key = kv.first;
        std::visit(
            mongo::OverloadedVisitor{
                [&](const std::string& str) { keyValues.push_back(getQueryParamsStr(key, str)); },
                [&](const boost::intrusive_ptr<mongo::Expression>& expr) {
                    auto paramVal = expr->evaluate(doc, &_context->expCtx->variables);
                    uassert(mongo::ErrorCodes::StreamProcessorInvalidOptions,
                            fmt::format("Expected {}.{} values to evaluate as a number, "
                                        "string, or boolean.",
                                        kHttpsStageName,
                                        HttpsOptions::kParametersFieldName),
                            paramVal.getType() == mongo::BSONType::String ||
                                paramVal.getType() == mongo::BSONType::Bool || paramVal.numeric());

                    if (paramVal.getType() == mongo::BSONType::String) {
                        // Parse strings separately to avoid unnecessary quotations
                        // surrounding the string output of `.toString()`.
                        keyValues.push_back(getQueryParamsStr(key, paramVal.getString()));
                    } else {
                        keyValues.push_back(getQueryParamsStr(key, paramVal.toString()));
                    }
                }},
            kv.second);
    }

    return keyValues;
}

boost::optional<std::string> HttpsOperator::parseContentTypeFromHeaders(StringData rawHeaders) {
    auto headerTokens = StringSplitter::split(rawHeaders.data(), "\r\n");
    for (const auto& headerToken : headerTokens) {
        auto keyAndValue = StringSplitter::split(headerToken, ": ");
        if (keyAndValue.size() != 2) {
            // There is network related data before the actual headers skip this.
            continue;
        }
        std::transform(
            keyAndValue[0].begin(), keyAndValue[0].end(), keyAndValue[0].begin(), ::tolower);
        if (keyAndValue[0] == "content-type") {
            auto contentType = StringSplitter::split(keyAndValue[1], ";");
            transform(
                contentType[0].begin(), contentType[0].end(), contentType[0].begin(), ::tolower);
            return contentType[0];
        }
    }
    return boost::none;
}

mongo::Document HttpsOperator::makeDocumentWithAPIResponse(const mongo::Document& inputDoc,
                                                           mongo::Value apiResponse) {
    MutableDocument output(std::move(inputDoc));
    output.setNestedField(_options.as, std::move(apiResponse));
    return output.freeze();
}

void HttpsOperator::tryLog(int id, std::function<void(int logID)> logFn) {
    if (!_logIDToRateLimiter.contains(id)) {
        _logIDToRateLimiter[id] = std::make_unique<RateLimiter>(kTryLogRate, 1, &_options.timer);
    }

    if (_logIDToRateLimiter[id]->consume() > Seconds(0)) {
        return;
    }

    logFn(id);
}

void HttpsOperator::parseBaseUrl() {
    char *scheme{nullptr}, *user{nullptr}, *password{nullptr}, *host{nullptr}, *port{nullptr},
        *path{nullptr}, *query{nullptr}, *fragment{nullptr};
    auto handle = curl_url();

    tassert(9617501, "Failed to initialize curl url handle", handle != nullptr);
    ScopeGuard guard([&] {
        curl_free(scheme);
        curl_free(user);
        curl_free(password);
        curl_free(host);
        curl_free(port);
        curl_free(path);
        curl_free(query);
        curl_free(fragment);
        curl_url_cleanup(handle);
    });

    UrlComponents out{};
    auto result = curl_url_set(handle, CURLUPART_URL, _options.url.c_str(), 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions, "Parsing url failed.", result == CURLUE_OK);

    result = curl_url_get(handle, CURLUPART_SCHEME, &scheme, 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Parsing url scheme failed or is not defined",
            result == CURLUE_OK && scheme != nullptr);
    if (scheme != nullptr) {
        out.scheme = std::string{scheme};
    }

    result = curl_url_get(handle, CURLUPART_USER, &user, 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Parsing url username failed",
            result == CURLUE_OK || result == CURLUE_NO_USER);
    if (user != nullptr) {
        out.user = std::string{user};
    }

    result = curl_url_get(handle, CURLUPART_PASSWORD, &password, 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Parsing url password failed",
            result == CURLUE_OK || result == CURLUE_NO_PASSWORD);
    if (password != nullptr) {
        out.password = std::string{password};
    }

    result = curl_url_get(handle, CURLUPART_HOST, &host, 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Parsing url hostname failed",
            result == CURLUE_OK);
    uassert(ErrorCodes::StreamProcessorInvalidOptions, "Parsing url failed.", host != nullptr);
    if (host != nullptr) {
        out.host = std::string{host};
    }

    result = curl_url_get(handle, CURLUPART_PORT, &port, 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Parsing url hostname failed",
            result == CURLUE_OK || result == CURLUE_NO_PORT);
    if (port != nullptr) {
        out.port = std::string{port};
    }

    result = curl_url_get(handle, CURLUPART_PATH, &path, 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Parsing url hostname failed",
            result == CURLUE_OK);
    if (path != nullptr) {
        out.path = std::string{path};
    }

    result = curl_url_get(handle, CURLUPART_QUERY, &query, 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "Parsing url query params failed",
            result == CURLUE_OK || result == CURLUE_NO_QUERY);
    if (query != nullptr) {
        std::vector<std::string> queryParamStrings;
        str::splitStringDelim(std::string{query}, &queryParamStrings, '&');
        for (const std::string& kv : queryParamStrings) {
            validateQueryParam(kv);
        }
        out.queryParams = std::move(queryParamStrings);
    }

    result = curl_url_get(handle, CURLUPART_FRAGMENT, &fragment, 0);
    uassert(9617510,
            "Parsing url fragment failed",
            result == CURLUE_OK || result == CURLUE_NO_FRAGMENT);
    if (fragment != nullptr) {
        out.fragment = std::string{fragment};
    }

    _baseUrlComponents = out;
}

std::string HttpsOperator::makeUrlString(std::string additionalPath,
                                         std::vector<std::string> additionalQueryParams) {
    CURLU* handle = curl_url();
    ScopeGuard guard([&] { curl_url_cleanup(handle); });

    CURLUcode result;

    result = curl_url_set(handle, CURLUPART_SCHEME, _baseUrlComponents.scheme.c_str(), 0);
    uassert(
        ErrorCodes::StreamProcessorInvalidOptions, "Failed to set scheme.", result == CURLUE_OK);

    if (!_baseUrlComponents.user.empty()) {
        result = curl_url_set(handle, CURLUPART_USER, _baseUrlComponents.user.c_str(), 0);
        uassert(
            ErrorCodes::StreamProcessorInvalidOptions, "Failed to set user.", result == CURLUE_OK);
    }
    if (!_baseUrlComponents.password.empty()) {
        result = curl_url_set(handle, CURLUPART_PASSWORD, _baseUrlComponents.password.c_str(), 0);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Failed to set password.",
                result == CURLUE_OK);
    }

    result = curl_url_set(handle, CURLUPART_HOST, _baseUrlComponents.host.c_str(), 0);
    uassert(ErrorCodes::StreamProcessorInvalidOptions, "Failed to set host.", result == CURLUE_OK);

    if (!_baseUrlComponents.port.empty()) {
        result = curl_url_set(handle, CURLUPART_PORT, _baseUrlComponents.port.c_str(), 0);
        uassert(
            ErrorCodes::StreamProcessorInvalidOptions, "Failed to set port.", result == CURLUE_OK);
    }

    if (!_baseUrlComponents.path.empty() || !additionalPath.empty()) {
        auto path = joinPaths(_baseUrlComponents.path, additionalPath);
        result = curl_url_set(handle, CURLUPART_PATH, path.c_str(), CURLU_URLENCODE);
        uassert(
            ErrorCodes::StreamProcessorInvalidOptions, "Failed to set path.", result == CURLUE_OK);
    }

    if (!_baseUrlComponents.queryParams.empty() || !additionalQueryParams.empty()) {
        additionalQueryParams.reserve(_baseUrlComponents.queryParams.size() +
                                      additionalQueryParams.size());
        if (!_baseUrlComponents.queryParams.empty()) {
            additionalQueryParams.insert(additionalQueryParams.begin(),
                                         _baseUrlComponents.queryParams.begin(),
                                         _baseUrlComponents.queryParams.end());
        }
        std::string queryString = urlEncodeAndJoinQueryParams(additionalQueryParams);
        result = curl_url_set(handle, CURLUPART_QUERY, queryString.c_str(), 0);
        uassert(
            ErrorCodes::StreamProcessorInvalidOptions, "Failed to set query.", result == CURLUE_OK);
    }

    if (!_baseUrlComponents.fragment.empty()) {
        result = curl_url_set(
            handle, CURLUPART_FRAGMENT, _baseUrlComponents.fragment.c_str(), CURLU_URLENCODE);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Failed to set fragment.",
                result == CURLUE_OK);
    }

    char* out{nullptr};
    ScopeGuard g([&] { curl_free(out); });
    result = curl_url_get(handle, CURLUPART_URL, &out, 0);
    tassert(9617515, "Failed to build url.", result == CURLUE_OK);
    return std::string{out};
}

void HttpsOperator::validateOptions() {
    bool baseUrlHasQuery = _baseUrlComponents.queryParams.size() > 0;
    bool baseUrlHasFragment = !_baseUrlComponents.fragment.empty();

    bool pathOptDefined = _options.pathExpr != nullptr;
    bool queryOptDefined = !_options.queryParams.empty();

    // These validations generally prevent users from making the operator interpolate parts of
    // an url that the base url has already defined a later part for. For example, appending path on
    // a base url that contains query params.
    // ie: adding "/foo/bar" path to "http://localhost:80?foo=bar".
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            fmt::format("The url defined in {}.connection.url contains a fragment that can "
                        "conflict with a query "
                        "or url path in the operator definition.",
                        kHttpsStageName),
            !(baseUrlHasFragment && (pathOptDefined || queryOptDefined)));
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            fmt::format("The url defined in {}.connection.url contains query parameters that can "
                        "conflict with a "
                        "url path in the operator definition.",
                        kHttpsStageName),
            !(baseUrlHasQuery && pathOptDefined));

    if (!mongo::getTestCommandsEnabled()) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                fmt::format("The url defined in {}.connection.url must use https", kHttpsStageName),
                _baseUrlComponents.scheme == kHttpsScheme);
    }

    for (const std::string& headerValue : _options.connectionHeaders) {
        auto headerEndIdx = headerValue.find(':');
        auto key = headerValue.substr(0, headerEndIdx);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                fmt::format("Keys defined in {}.connection.headers must not be empty.",
                            kHttpsStageName),
                !key.empty());
    }
    for (const auto& headers : _options.operatorHeaders) {
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                fmt::format("Keys defined in {}.headers must not be empty.", kHttpsStageName),
                !headers.first.empty());
    }
}

std::string HttpsOperator::joinPaths(const std::string& basePath, const std::string& path) {
    auto isEmpty = [](const std::string& s) { return s.empty() || s == "/"; };
    // removes preceding and trailing slashes
    auto cleanSlashes = [isEmpty](const std::string& s) {
        if (isEmpty(s)) {
            return s;
        }

        std::string out = s;
        if (out.front() == '/') {
            out = out.substr(1);
        }
        if (out.back() == '/') {
            out.pop_back();
        }
        return out;
    };

    bool basePathIsEmpty = isEmpty(basePath);
    bool pathIsEmpty = isEmpty(path);

    std::string out = "/";
    if (basePathIsEmpty && pathIsEmpty) {
        return out;
    }
    if (basePathIsEmpty) {
        return out + cleanSlashes(path);
    }
    if (pathIsEmpty) {
        return out + cleanSlashes(basePath);
    }

    // At this point, we need to join 2 non-empty paths.
    out.append(cleanSlashes(basePath));
    out.push_back('/');
    out.append(cleanSlashes(path));

    return out;
}

int64_t getRateLimitPerSec(boost::optional<StreamProcessorFeatureFlags> featureFlags) {
    tassert(9503701, "Feature flags should be set", featureFlags);
    auto val = featureFlags->getFeatureFlagValue(FeatureFlags::kHttpsRateLimitPerSecond).getInt();

    return *val;
}

}  // namespace streams
//
