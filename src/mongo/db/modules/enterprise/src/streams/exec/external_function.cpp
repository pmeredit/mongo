/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_function.h"

#include <aws/lambda/model/InvokeRequest.h>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <cstdio>
#include <exception>
#include <fmt/core.h>
#include <memory>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/json.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/str.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/operator.h"
#include "streams/exec/rate_limiter.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

// These failpoints throw an exception after a successful invoke simulating lambda errors.
MONGO_FAIL_POINT_DEFINE(awsLambdaNotFoundAtConnectionValidation);
MONGO_FAIL_POINT_DEFINE(awsUserFunctionError);
MONGO_FAIL_POINT_DEFINE(awsLambdaNotFoundAtProcessDocument);
MONGO_FAIL_POINT_DEFINE(awsLambdaBadRequestContent);

using namespace mongo;

void ExternalFunction::doRegisterMetrics(MetricManager* metricManager) {
    auto defaultLabels = getDefaultMetricLabels(_context);

    _throttleDurationCounter =
        metricManager->registerCounter("external_function_operator_throttle_duration_micros",
                                       "Time slept by the external_function operator to not exceed "
                                       "the rate limiter in microseconds",
                                       defaultLabels);

    constexpr StringData resultLabelKey{"result"};
    auto successfulRequestTimeLabels = defaultLabels;
    successfulRequestTimeLabels.push_back(std::make_pair(resultLabelKey.toString(), "success"));
    _successfulRequestTimeHistogram = metricManager->registerHistogram(
        "external_function_operator_aws_sdk_request_time",
        "AWS SDK request round-trip time",
        successfulRequestTimeLabels,
        makeExponentialDurationBuckets(kRequestTimeHistogramBucketStartMs,
                                       kRequestTimeHistogramExpFactor,
                                       kRequestTimeHistogramBucketCount));

    auto failedRequestTimeLabels = defaultLabels;
    failedRequestTimeLabels.push_back(std::make_pair(resultLabelKey.toString(), "fail"));
    _failedRequestTimeHistogram = metricManager->registerHistogram(
        "external_function_operator_aws_sdk_request_time",
        "AWS SDK request round-trip time",
        failedRequestTimeLabels,
        makeExponentialDurationBuckets(kRequestTimeHistogramBucketStartMs,
                                       kRequestTimeHistogramExpFactor,
                                       kRequestTimeHistogramBucketCount));

    _responseCodesCounterVec = metricManager->registerCounterVec(
        "external_function_operator_request_counter",
        "The number of occasions a request has completed a a roundtrip to AWS",
        std::move(defaultLabels),
        {"response_code"});
}

ExternalFunction::ExternalFunction(Context* context,
                                   ExternalFunction::Options options,
                                   std::string operatorName,
                                   int64_t rateLimitPerSec)
    : _context(context),
      _options(std::move(options)),
      _operatorName{operatorName},
      _rateLimitPerSec{rateLimitPerSec},
      _rateLimiter{_rateLimitPerSec, &_options.timer} {}

const ExternalFunction::Options& ExternalFunction::getOptions() {
    return _options;
}

void ExternalFunction::writeToDLQ(const StreamDocument* streamDoc,
                                  const mongo::Document& payloadDoc,
                                  const std::string& errorMsg,
                                  ProcessResult& result) {
    const std::string dlqErrorMsg = str::stream()
        << "Failed to process input document in " << kExternalFunctionStageName
        << " with error: " << errorMsg;
    result.numDlqBytes +=
        _context->dlq->addMessage(toDeadLetterQueueMsg(_context->streamMetaFieldName,
                                                       streamDoc->streamMeta,
                                                       payloadDoc,
                                                       _operatorName,
                                                       std::move(dlqErrorMsg)));
    result.numDlqDocs++;
}

void ExternalFunction::writeToStreamMeta(StreamDocument* streamDoc,
                                         const std::string& functionName,
                                         const Aws::Lambda::Model::InvokeResult& invokeResult,
                                         double responseTimeMs) {
    StreamMetaExternalFunction streamMetaExternalFunction;
    streamMetaExternalFunction.setFunctionName(functionName);
    streamMetaExternalFunction.setExecutedVersion(invokeResult.GetExecutedVersion());
    streamMetaExternalFunction.setStatusCode(invokeResult.GetStatusCode());
    streamMetaExternalFunction.setResponseTimeMs(responseTimeMs);

    streamDoc->streamMeta.setExternalFunction(std::move(streamMetaExternalFunction));
    streamDoc->onMetaUpdate(_context);
}


bool ExternalFunction::handleDataException(const std::string& msgPrefix,
                                           const std::exception& e,
                                           const StreamDocument* streamDoc,
                                           const mongo::Document& payloadDoc,
                                           ProcessResult& processResult) {
    switch (_options.onError) {
        case mongo::OnErrorEnum::DLQ: {
            writeToDLQ(
                streamDoc,
                payloadDoc,
                fmt::format(
                    "{} in {} with error: {}", msgPrefix, kExternalFunctionStageName, e.what()),
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
            tasserted(9929403, "Invalid onError option set");
            break;
        }
    }
}

void ExternalFunction::validateFunctionConnection() {
    Aws::Lambda::LambdaError lambdaError;
    try {
        if (mongo::getTestCommandsEnabled()) {
            LOGV2_INFO(9929600,
                       "Validating external function connection.",
                       "context"_attr = _context,
                       "functionName"_attr = _options.functionName);
        }

        Aws::Lambda::Model::InvokeRequest request;
        request.SetFunctionName(_options.functionName);
        request.SetInvocationType(Aws::Lambda::Model::InvocationType::DryRun);

        Aws::Lambda::Model::InvokeOutcome outcome = _options.lambdaClient->Invoke(request);

        if (MONGO_unlikely(awsLambdaNotFoundAtConnectionValidation.shouldFail())) {
            outcome = simulateLambdaNotFound();
        }

        if (outcome.IsSuccess()) {
            return;
        }
        // AWS Error
        lambdaError = outcome.GetError();
        uasserted(ErrorCodes::StreamProcessorExternalFunctionConnectionError,
                  fmt::format("Received error response from external function. Error: {}",
                              lambdaError.GetMessage()));

    } catch (const ExceptionFor<ErrorCodes::StreamProcessorExternalFunctionConnectionError>& e) {
        tryLog(9929601, [&](int logID) {
            LOGV2_INFO(logID,
                       "Error occurred while validating external function connection.",
                       "context"_attr = _context,
                       "error"_attr = e.what(),
                       "code"_attr = lambdaError.GetResponseCode());
        });
        // prevent error when retrieving error type.
        if (!lambdaError.GetRequestId().empty()) {
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    e.what(),
                    lambdaError.GetErrorType() != Aws::Lambda::LambdaErrors::RESOURCE_NOT_FOUND);
        }
        throw;
    }
}

ExternalFunction::ProcessResult ExternalFunction::processStreamDoc(StreamDocument* streamDoc) {
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
        tassert(9929401, "Expected result doc to exist", result);
    }

    const mongo::Document& payloadDoc = result ? *result : streamDoc->doc;
    auto& inputDoc = streamDoc->doc;

    std::string rawDoc;
    try {
        rawDoc = serializeJson(payloadDoc.toBson(), JsonStringFormat::Relaxed);
    } catch (ExceptionFor<ErrorCodes::BSONObjectTooLarge>& e) {
        writeToDLQ(
            streamDoc, payloadDoc, fmt::format("{}: {}", e.codeString(), e.what()), processResult);
        return processResult;
    }
    processResult.numOutputBytes += rawDoc.size();

    // Wait for throttle delay before performing request
    auto throttleDelay = _rateLimiter.consume();
    if (throttleDelay > Microseconds(0)) {
        _options.throttleFn(throttleDelay);
        _throttleDurationCounter->increment(durationCount<Microseconds>(throttleDelay));
        tassert(
            9929402, "Expected a zero throttle delay", _rateLimiter.consume() == Microseconds(0));
    }

    Aws::Lambda::Model::InvokeRequest request;
    request.SetFunctionName(_options.functionName);
    std::shared_ptr<Aws::IOStream> payload = std::make_shared<Aws::StringStream>(std::move(rawDoc));
    request.SetBody(payload);
    request.SetContentType(kApplicationJson.data());
    if (_options.execution == mongo::ExternalFunctionExecutionEnum::Sync) {
        request.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
    } else {
        request.SetInvocationType(Aws::Lambda::Model::InvocationType::Event);
    }

    double responseTimeMs{0};
    std::string rawResponse;
    Aws::Lambda::Model::InvokeResult invokeResult;
    Aws::Lambda::LambdaError lambdaError;
    Timer timer{};
    try {
        if (mongo::getTestCommandsEnabled()) {
            LOGV2_INFO(9929404,
                       "Making external function call.",
                       "context"_attr = _context,
                       "functionName"_attr = _options.functionName);
        }
        Aws::Lambda::Model::InvokeOutcome outcome = _options.lambdaClient->Invoke(request);
        responseTimeMs = durationCount<Milliseconds>(timer.elapsed());

        if (MONGO_unlikely(awsUserFunctionError.shouldFail())) {
            outcome = simulateUserFunctionError();
        }
        if (MONGO_unlikely(awsLambdaNotFoundAtProcessDocument.shouldFail())) {
            outcome = simulateLambdaNotFound();
        }
        if (MONGO_unlikely(awsLambdaBadRequestContent.shouldFail())) {
            outcome = simulateBadRequestContent();
        }

        if (outcome.IsSuccess()) {
            _successfulRequestTimeHistogram->increment(responseTimeMs);

            invokeResult = outcome.GetResultWithOwnership();
            _responseCodesCounterVec->withLabels({std::to_string(invokeResult.GetStatusCode())})
                ->increment();
            if (_options.execution == mongo::ExternalFunctionExecutionEnum::Sync) {
                std::stringstream tempStream;
                tempStream << invokeResult.GetPayload().rdbuf();
                rawResponse = tempStream.str();
                processResult.numInputBytes += rawResponse.size();
                if (!invokeResult.GetFunctionError().empty()) {
                    uasserted(
                        ErrorCodes::StreamProcessorExternalFunctionConnectionError,
                        fmt::format("Received error response from external function. Payload: {}",
                                    rawResponse));
                }
            }
        } else {
            // AWS Error
            _failedRequestTimeHistogram->increment(responseTimeMs);

            lambdaError = outcome.GetError();
            _responseCodesCounterVec
                ->withLabels({std::to_string(static_cast<int>(lambdaError.GetResponseCode()))})
                ->increment();
            uasserted(ErrorCodes::StreamProcessorExternalFunctionConnectionError,
                      fmt::format("Received error response from external function. Error: {}",
                                  lambdaError.GetMessage()));
        }
    } catch (const ExceptionFor<ErrorCodes::StreamProcessorExternalFunctionConnectionError>& e) {
        tryLog(9929405, [&](int logID) {
            LOGV2_INFO(logID,
                       "Error occurred while performing request in ExternalFunction",
                       "context"_attr = _context,
                       "error"_attr = e.what(),
                       "code"_attr = lambdaError.GetResponseCode());
        });
        // prevent error when retrieving error type.
        if (!lambdaError.GetRequestId().empty()) {
            uassert(ErrorCodes::StreamProcessorInvalidOptions,
                    e.what(),
                    lambdaError.GetErrorType() != Aws::Lambda::LambdaErrors::RESOURCE_NOT_FOUND);
            if (shouldFail(lambdaError)) {
                throw;
            }
        }
        if (handleDataException("Request failure", e, streamDoc, payloadDoc, processResult)) {
            return processResult;
        }
    }

    if (_options.isSink) {
        return processResult;
    }

    mongo::Value responseAsValue;
    if (!rawResponse.empty()) {
        if (!rawResponse.starts_with('{') && !rawResponse.starts_with('[')) {
            responseAsValue = Value(rawResponse);
        } else {
            try {
                responseAsValue = parseAndDeserializeJsonResponse(rawResponse, false);
            } catch (const bsoncxx::exception& e) {
                tryLog(9929407, [&](int logID) {
                    LOGV2_INFO(logID,
                               "Error occured while reading response in ExternalFunction",
                               "context"_attr = _context,
                               "error"_attr = e.what());
                });
                if (handleDataException(
                        "Failed to parse response body", e, streamDoc, payloadDoc, processResult)) {
                    return processResult;
                }
            }
        }
    }

    writeToStreamMeta(streamDoc, _options.functionName, invokeResult, responseTimeMs);
    if (!responseAsValue.missing()) {
        auto joinedDoc = makeDocumentWithAPIResponse(inputDoc, std::move(responseAsValue));
        streamDoc->doc = std::move(joinedDoc);
    }
    processResult.dataMsgBytes = streamDoc->doc.getApproximateSize();
    processResult.addDocToOutputMsg = true;

    return processResult;
}

mongo::Document ExternalFunction::makeDocumentWithAPIResponse(const mongo::Document& inputDoc,
                                                              mongo::Value response) {
    tassert(9929411,
            "Expected as to be unset",
            _options.execution == mongo::ExternalFunctionExecutionEnum::Sync &&
                _options.as != boost::none && !_options.as->empty());
    MutableDocument output(inputDoc);
    output.setNestedField(*_options.as, std::move(response));
    return output.freeze();
}

bool ExternalFunction::shouldFail(const Aws::Lambda::LambdaError& lambdaError) {
    if (lambdaError.GetResponseCode() == Aws::Http::HttpResponseCode::BAD_REQUEST &&
        (lambdaError.GetErrorType() == Aws::Lambda::LambdaErrors::INVALID_REQUEST_CONTENT ||
         lambdaError.GetErrorType() == Aws::Lambda::LambdaErrors::THROTTLING)) {
        return true;

    } else if (lambdaError.GetResponseCode() == Aws::Http::HttpResponseCode::GONE &&
               lambdaError.GetErrorType() == Aws::Lambda::LambdaErrors::E_F_S_I_O) {
        return true;
    } else if (lambdaError.GetResponseCode() == Aws::Http::HttpResponseCode::BAD_REQUEST ||
               lambdaError.GetResponseCode() == Aws::Http::HttpResponseCode::GONE ||
               lambdaError.GetResponseCode() ==
                   Aws::Http::HttpResponseCode::REQUEST_ENTITY_TOO_LARGE ||
               lambdaError.GetResponseCode() == Aws::Http::HttpResponseCode::REQUEST_URI_TOO_LONG ||
               lambdaError.GetResponseCode() ==
                   Aws::Http::HttpResponseCode::REQUEST_HEADER_FIELDS_TOO_LARGE) {
        return false;
    }
    return true;
};

void ExternalFunction::tryLog(int id, std::function<void(int logID)> logFn) {
    if (!_logIDToRateLimiter.contains(id)) {
        _logIDToRateLimiter[id] = std::make_unique<RateLimiter>(kTryLogRate, 1, &_options.timer);
    }

    if (_logIDToRateLimiter[id]->consume() > Seconds(0)) {
        return;
    }

    logFn(id);
}

void ExternalFunction::updateRateLimitPerSecond(int64_t newRateLimitPerSec) {
    if (newRateLimitPerSec == _rateLimitPerSec) {
        return;
    }
    _rateLimitPerSec = newRateLimitPerSec;
    _rateLimiter.setTokensRefilledPerSecAndCapacity(newRateLimitPerSec, newRateLimitPerSec);
}

Aws::Lambda::Model::InvokeOutcome ExternalFunction::simulateUserFunctionError() {
    Aws::Lambda::Model::InvokeResult result;
    result.SetStatusCode(200);
    result.SetExecutedVersion("$LATEST");
    result.SetFunctionError("Unhandled");
    auto responseBody = Aws::Utils::Stream::DefaultResponseStreamFactoryMethod();
    *responseBody << "Uncaught User Exception";
    result.ReplaceBody(responseBody);
    return Aws::Lambda::Model::InvokeOutcome(std::move(result));
}

Aws::Lambda::Model::InvokeOutcome ExternalFunction::simulateLambdaNotFound() {
    Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
        Aws::Lambda::LambdaErrors::RESOURCE_NOT_FOUND, Aws::Client::RetryableType::NOT_RETRYABLE);
    error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
    error.SetMessage("Not Found");
    error.SetRequestId("r1");
    return Aws::Lambda::Model::InvokeOutcome(std::move(error));
}

Aws::Lambda::Model::InvokeOutcome ExternalFunction::simulateBadRequestContent() {
    Aws::Lambda::LambdaError error = Aws::Client::AWSError<Aws::Lambda::LambdaErrors>(
        Aws::Lambda::LambdaErrors::INVALID_REQUEST_CONTENT,
        Aws::Client::RetryableType::NOT_RETRYABLE);
    error.SetResponseCode(Aws::Http::HttpResponseCode::BAD_REQUEST);
    error.SetMessage("Invalid Request Content");
    error.SetRequestId("r1");
    return Aws::Lambda::Model::InvokeOutcome(std::move(error));
}

}  // namespace streams
//
