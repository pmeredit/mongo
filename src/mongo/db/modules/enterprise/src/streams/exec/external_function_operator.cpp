/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_function_operator.h"

#include <algorithm>
#include <aws/lambda/model/InvokeRequest.h>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <cstdio>
#include <fmt/core.h>

#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/operator.h"
#include "streams/exec/stream_stats.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {
using namespace mongo;

void ExternalFunctionOperator::registerMetrics(MetricManager* metricManager) {
    _externalFunction.doRegisterMetrics(metricManager);
}

ExternalFunctionOperator::ExternalFunctionOperator(Context* context,
                                                   ExternalFunction::Options options)
    : Operator(context, 1, 1),
      _externalFunction{context,
                        std::move(options),
                        kName.toString(),
                        getExternalFunctionRateLimitPerSec(context->featureFlags)} {
    _stats.connectionType = ConnectionTypeEnum::AWSIAMLambda;
}

void ExternalFunctionOperator::doStart() {
    tassert(
        992600, "isSink should be false for middle stage", !_externalFunction.getOptions().isSink);
    _externalFunction.validateFunctionConnection();
}

const ExternalFunction::Options& ExternalFunctionOperator::getOptions() {
    return _externalFunction.getOptions();
}

void ExternalFunctionOperator::doOnDataMsg(int32_t inputIdx,
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

    _externalFunction.updateRateLimitPerSecond(
        getExternalFunctionRateLimitPerSec(_context->featureFlags));

    for (auto& streamDoc : dataMsg.docs) {
        if (outputMsg.docs.size() == kDataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
            sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg));
            curDataMsgByteSize = 0;
            outputMsg = StreamDataMsg{};
            outputMsg.docs.reserve(outputMsgDocsSize);
        }

        auto result = _externalFunction.processStreamDoc(&streamDoc);
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
                      .timeSpent = dataMsg.creationTimer.elapsed()});
    sendDataMsg(/*outputIdx*/ 0, std::move(outputMsg), std::move(controlMsg));
}


void ExternalFunctionOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // Let the control message flow to the next operator.
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

}  // namespace streams
//
