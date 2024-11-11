/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_api_operator.h"

#include <algorithm>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <cstdio>
#include <curl/curl.h>
#include <curl/easy.h>
#include <curl/urlapi.h>
#include <iostream>
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
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/net/http_client.h"
#include "mongo/util/overloaded_visitor.h"
#include "mongo/util/str.h"
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

HttpMethodEnum httpMethodClientToIDLType(HttpClient::HttpMethod requestType) {
    switch (requestType) {
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
            tasserted(ErrorCodes::InternalError, "Unknown requestType");
    }
}
}  // namespace

using namespace mongo;

void ExternalApiOperator::registerMetrics(MetricManager* metricManager) {
    _throttleDurationCounter = metricManager->registerCounter(
        "rest_operator_throttle_duration_micros",
        "Time slept by the rest operator to not exceed the rate limiter in microseconds",
        getDefaultMetricLabels(_context));
}

ExternalApiOperator::ExternalApiOperator(Context* context, ExternalApiOperator::Options options)
    : Operator(context, 1, 1),
      _options(std::move(options)),
      _rateLimitPerSec{getRateLimitPerSec(*context->featureFlags)},
      _rateLimiter{_rateLimitPerSec, &_options.timer},
      _cidrDenyList{parseCidrDenyList()} {
    _stats.connectionType = ConnectionTypeEnum::WebAPI;
}

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
                      .numDlqBytes = numDlqBytes});
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}

void ExternalApiOperator::writeToStreamMeta(StreamDocument* streamDoc,
                                            const std::string& requestUrl,
                                            const mongo::HttpClient::HttpReply httpResponse,
                                            double responseTimeMs) {
    StreamMetaExternalAPI streamMetaExternalAPI;
    streamMetaExternalAPI.setUrl(requestUrl);
    streamMetaExternalAPI.setRequestType(httpMethodClientToIDLType(_options.requestType));
    streamMetaExternalAPI.setHttpStatusCode(httpResponse.code);
    streamMetaExternalAPI.setResponseTimeMs(responseTimeMs);

    streamDoc->streamMeta.setExternalAPI(std::move(streamMetaExternalAPI));

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

void ExternalApiOperator::writeToDLQ(StreamDocument* streamDoc,
                                     const std::string& errorMsg,
                                     ProcessResult& result) {
    const std::string dlqErrorMsg = str::stream()
        << "Failed to process input document in " << getName() << " with error: " << errorMsg;
    result.numDlqBytes += _context->dlq->addMessage(toDeadLetterQueueMsg(
        _context->streamMetaFieldName, std::move(*streamDoc), doGetName(), std::move(dlqErrorMsg)));
    result.numDlqDocs++;
}

void ExternalApiOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // Let the control message flow to the next operator.
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

ExternalApiOperator::ProcessResult ExternalApiOperator::processStreamDoc(
    StreamDocument* streamDoc) {
    auto& inputDoc = streamDoc->doc;
    boost::optional<mongo::HttpClient::HttpReply> httpResponse;

    ProcessResult processResult{};

    std::string requestUrl;
    std::vector<std::string> headers;
    try {
        requestUrl = (_options.urlPathExpr || _options.queryParams.size() > 0)
            ? evaluateFullUrl(streamDoc->doc)
            : _options.url;
        headers = evaluateHeaders(inputDoc);
    } catch (const DBException& e) {
        writeToDLQ(streamDoc, e.what(), processResult);
        return processResult;
    }

    std::string rawDoc;
    switch (_options.requestType) {
        case HttpClient::HttpMethod::kPOST:
        case HttpClient::HttpMethod::kPUT:
        case HttpClient::HttpMethod::kPATCH: {
            rawDoc = tojson(inputDoc.toBson(), mongo::JsonStringFormat::ExtendedRelaxedV2_0_0);
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

    double responseTimeMs{0};
    StringData rawResponse;
    try {
        Timer timer{};

        httpResponse = _options.httpClient->request(_options.requestType, requestUrl, data);
        tassert(9502901, "Expected HTTP response to be set", httpResponse);

        // readAndAdvance can fail to parse response body and throw an exception.
        rawResponse = httpResponse->body.getCursor().readAndAdvance<StringData>();

        responseTimeMs = durationCount<Milliseconds>(timer.elapsed());
        processResult.numInputBytes += rawResponse.size();

        uassert(ErrorCodes::OperationFailed,
                fmt::format("Received error response from server. status code: {}, body: {}",
                            httpResponse->code,
                            rawResponse),
                httpResponse->code >= 200 && httpResponse->code < 300);
    } catch (const DBException& e) {
        switch (_options.onError) {
            case mongo::OnErrorEnum::DLQ: {
                writeToDLQ(streamDoc, e.what(), processResult);
                return processResult;
            }
            case mongo::OnErrorEnum::Ignore: {
                break;
            }
            case mongo::OnErrorEnum::Fail: {
                throw;
            }
            default: {
                tasserted(9503300, "Invalid onError option set");
                break;
            }
        }
    }

    mongo::BSONObj apiResponse;
    try {
        if (rawResponse.size()) {
            // TODO(SERVER-96836): Determine the response body format via the content-type header
            // and add support for plaintext responses
            auto responseView =
                bsoncxx::stdx::string_view{std::move(rawResponse.data()), rawResponse.size()};
            apiResponse = fromBsoncxxDocument(bsoncxx::from_json(responseView));
        }
    } catch (const bsoncxx::exception& e) {
        switch (_options.onError) {
            case mongo::OnErrorEnum::DLQ: {
                writeToDLQ(streamDoc,
                           str::stream() << "Failed to parse response body in " << getName()
                                         << " with error: " << e.what(),
                           processResult);
                return processResult;
            }
            case mongo::OnErrorEnum::Ignore: {
                break;
            }
            case mongo::OnErrorEnum::Fail: {
                throw;
            }
            default: {
                tasserted(9503301, "Invalid onError option set");
                break;
            }
        }
    }

    writeToStreamMeta(streamDoc, requestUrl, std::move(*httpResponse), responseTimeMs);
    auto joinedDoc = makeDocumentWithAPIResponse(inputDoc, Value(std::move(apiResponse)));
    processResult.dataMsgBytes = joinedDoc.getApproximateSize();
    processResult.addDocToOutputMsg = true;
    streamDoc->doc = std::move(joinedDoc);

    return processResult;
}

std::string ExternalApiOperator::evaluateFullUrl(const mongo::Document& doc) {
    // TODO(SERVER-96880) url encode the user-provided url.
    std::string out = _options.url;
    if (_options.urlPathExpr) {
        out += evaluatePath(doc);
    }

    if (!_options.queryParams.empty()) {
        out += evaluateQueryParams(doc);
    }

    return out;
}

std::string ExternalApiOperator::evaluatePath(const mongo::Document& doc) {
    Value pathValue = _options.urlPathExpr->evaluate(doc, &_context->expCtx->variables);
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            fmt::format("Expected {}.{} to evaluate to a string.",
                        kExternalApiStageName,
                        ExternalAPIOptions::kUrlPathFieldName),
            pathValue.getType() == mongo::BSONType::String);
    return pathValue.getString();
}

std::vector<std::string> ExternalApiOperator::evaluateHeaders(const mongo::Document& doc) {
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
                                        kExternalApiStageName,
                                        ExternalAPIOptions::kHeadersFieldName),
                            headerVal.getType() == mongo::BSONType::String);
                    headers.emplace_back(getHeaderStr(kv.first, headerVal.getString()));
                }},
            kv.second);
    }
    return headers;
}

std::string ExternalApiOperator::evaluateQueryParams(const mongo::Document& doc) {
    if (_options.queryParams.empty()) {
        return "";
    }

    std::vector<std::string> keyValues{};
    keyValues.reserve(_options.queryParams.size());
    for (const auto& kv : _options.queryParams) {
        // Note: the planner parses out expressions that are represented using strings so if
        // operatorHeaders has a string value, it is guaranteed to be a static string.
        std::string key = urlEncode(kv.first);
        std::visit(
            mongo::OverloadedVisitor{
                [&](const std::string& str) {
                    keyValues.push_back(getQueryParamsStr(key, urlEncode(str)));
                },
                [&](const boost::intrusive_ptr<mongo::Expression>& expr) {
                    auto paramVal = expr->evaluate(doc, &_context->expCtx->variables);
                    uassert(mongo::ErrorCodes::StreamProcessorInvalidOptions,
                            fmt::format("Expected {}.{} values to evaluate as a number, "
                                        "string, or boolean.",
                                        kExternalApiStageName,
                                        ExternalAPIOptions::kParametersFieldName),
                            paramVal.getType() == mongo::BSONType::String ||
                                paramVal.getType() == mongo::BSONType::Bool || paramVal.numeric());

                    if (paramVal.getType() == mongo::BSONType::String) {
                        // Parse strings separately to avoid unnecessary quotations
                        // surrounding the string output of `.toString()`.
                        keyValues.push_back(
                            getQueryParamsStr(key, urlEncode(paramVal.getString())));
                    } else {
                        keyValues.push_back(getQueryParamsStr(key, urlEncode(paramVal.toString())));
                    }
                }},
            kv.second);
    }

    std::string out;
    str::joinStringDelim(keyValues, &out, '&');
    return "?" + std::move(out);
}

mongo::Document ExternalApiOperator::makeDocumentWithAPIResponse(const mongo::Document& inputDoc,
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
