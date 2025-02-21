/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/lambda/LambdaClient.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <string>

#include "mongo/base/string_data.h"
#include "streams/exec/context.h"
#include "streams/exec/external_function.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/queued_sink_operator.h"

namespace streams {

/**
 * ExternalFunctionSinkOperator is an operator that allows users to make requests to a configured
 * destination in response to input data. This only supports AWS Lambda as of today.
 *
 * This operator requires a AWS IAM connection and will make a request to that connection and set
 * the response to the user-configured 'as' field before sending the document to the next
 * operator.
 */
class ExternalFunctionSinkOperator : public QueuedSinkOperator {
public:
    static constexpr StringData kName = "ExternalFunctionSinkOperator";

    ExternalFunctionSinkOperator(Context* context, ExternalFunction::Options options);

    const ExternalFunction::Options& getOptions();

    void registerMetrics(MetricManager* metricManager) override;

    // Make a SinkWriter instance.
    std::unique_ptr<SinkWriter> makeWriter() override;

protected:
    mongo::ConnectionTypeEnum getConnectionType() const override {
        return *_stats.connectionType;
    };

    std::string doGetName() const override {
        return kName.toString();
    }

    bool shouldComputeInputByteStats() const override {
        return false;
    }

private:
    ExternalFunction _externalFunction;
};


/**
 * ExternalFunctionWriter implements the logic for $ExternalFunction requests to a external
 * function. One ExternalFunctionOperator (QueuedSinkOperator) might manages multiple
 * ExternalFunctionWriter instances if $ExternalFunction.parallelism > 1.
 */
class ExternalFunctionWriter : public SinkWriter {
public:
    ExternalFunctionWriter(Context* context,
                           SinkOperator* sinkOperator,
                           ExternalFunction* externalFunction);

protected:
    OperatorStats processDataMsg(StreamDataMsg dataMsg) override;

    void connect() override;

    size_t partition(StreamDocument& doc) override;

private:
    ExternalFunction* _externalFunction{nullptr};
};

}  // namespace streams
