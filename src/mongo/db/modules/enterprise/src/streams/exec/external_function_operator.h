/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/lambda/LambdaClient.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "aws_util.h"
#include "mongo/base/string_data.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"
#include "streams/exec/context.h"
#include "streams/exec/feedable_pipeline.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/rate_limiter.h"

namespace streams {

/**
 * ExternalFunctionOperator is an operator that allows users to make requests to a configured
 * destination in response to input data. This only supports AWS Lambda as of today.
 *
 * This operator requires a AWS IAM connection and will make a request to that connection and set
 * the response to the user-configured 'as' field before sending the document to the next
 * operator.
 */
class ExternalFunctionOperator : public Operator {
public:
    static constexpr StringData kName = "ExternalFunctionOperator";
    static constexpr StringData kExternalFunctionScheme = "https";
    static constexpr StringData kApplicationJson = "application/json";
    static constexpr StringData kTextPlain = "text/plain";

    struct Options {
        // Client to manage invoking AWS lambdas
        std::unique_ptr<LambdaClient> lambdaClient;
        // the function name or its full ARN.
        std::string functionName;
        // The execution flow to use when calling the function.
        mongo::ExternalFunctionExecutionEnum execution;
        // Represents the key that this operator will associate with the response value.
        boost::optional<std::string> as;
        // The payloadPipeline is to run against the dataMsg docs passed into the operator.
        boost::optional<FeedablePipeline> payloadPipeline;
        // throttleFn is a callback for the behavior that occurs when throttled
        std::function<void(Microseconds)> throttleFn{sleepFor<Microseconds>};

        // timer is the timer to be used for rate limiting
        Timer timer{};

        // Specifies how error responses are handled
        mongo::OnErrorEnum onError{mongo::OnErrorEnum::DLQ};
    };

    ExternalFunctionOperator(Context* context, Options options);

    const Options& getOptions();

    void registerMetrics(MetricManager* metricManager) override;

protected:
    std::string doGetName() const override {
        return kName.toString();
    }

    void doStart() override;

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;


private:
    friend class ExternalFunctionOperatorTest;

    static constexpr double kTryLogRate{1.0 / 60};

    struct ProcessResult {
        bool addDocToOutputMsg{false};
        int64_t dataMsgBytes{0};
        int64_t numDlqDocs{0};
        int64_t numDlqBytes{0};
        int64_t numInputBytes{0};
        int64_t numOutputBytes{0};
    };

    ProcessResult processStreamDoc(StreamDocument* streamDoc);

    // makeDocumentWithAPIResponse sets the api response as a value in the input document using
    // a user-configured key.
    mongo::Document makeDocumentWithAPIResponse(const mongo::Document& inputDoc,
                                                mongo::Value apiResponse);

    // writeToStreamMeta writes to a StreamDocuments' stream meta
    void writeToStreamMeta(StreamDocument* streamDoc,
                           const std::string& functionName,
                           const Aws::Lambda::Model::InvokeResult& invokeResult,
                           double responseTimeMs);

    // writeToDLQ writes StreamDocument to the DLQ with a specified error message and operator stats
    // for the given document
    void writeToDLQ(StreamDocument* streamDoc,
                    const mongo::Document& payloadDoc,
                    const std::string& errorMsg,
                    ProcessResult& result);

    // tryLog will only log if the given logID hasn't been used within the last minute
    void tryLog(int id, std::function<void(int logID)> logFn);

    // shouldFail is called to determine if we should use the onError behavior
    // vs failing the processor.
    bool shouldFail(const Aws::Lambda::LambdaError& lambdaError);

    // used in failpoint testing to simulate AWS behavior in jstests
    Aws::Lambda::Model::InvokeOutcome simulateUserFunctionError();
    Aws::Lambda::Model::InvokeOutcome simulateLambdaNotFound();
    Aws::Lambda::Model::InvokeOutcome simulateBadRequestContent();

    ExternalFunctionOperator::Options _options;

    int64_t _rateLimitPerSec;
    RateLimiter _rateLimiter;
    std::shared_ptr<Counter> _throttleDurationCounter;

    stdx::unordered_map<int, std::unique_ptr<RateLimiter>> _logIDToRateLimiter;
};

}  // namespace streams
