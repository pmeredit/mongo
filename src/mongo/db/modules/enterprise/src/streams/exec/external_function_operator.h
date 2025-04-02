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
#include <string>

#include "mongo/base/string_data.h"
#include "streams/exec/context.h"
#include "streams/exec/external_function.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"

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

    ExternalFunctionOperator(Context* context, ExternalFunction::Options options);

    const ExternalFunction::Options& getOptions();

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
    ExternalFunction _externalFunction;
};

}  // namespace streams
