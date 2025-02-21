/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/external_function_sink_operator.h"

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

void ExternalFunctionSinkOperator::registerMetrics(MetricManager* metricManager) {
    _externalFunction.doRegisterMetrics(metricManager);
    QueuedSinkOperator::registerMetrics(metricManager);
}

ExternalFunctionSinkOperator::ExternalFunctionSinkOperator(Context* context,
                                                           ExternalFunction::Options options)
    // The ratelimiter will need mutexes once mult-parallelism is supported.
    : QueuedSinkOperator(context, 1, 1),
      _externalFunction{context,
                        std::move(options),
                        kName.toString(),
                        getExternalFunctionRateLimitPerSec(context->featureFlags)} {
    _stats.connectionType = ConnectionTypeEnum::AWSIAMLambda;
}


const ExternalFunction::Options& ExternalFunctionSinkOperator::getOptions() {
    return _externalFunction.getOptions();
}

std::unique_ptr<SinkWriter> ExternalFunctionSinkOperator::makeWriter() {
    return std::make_unique<ExternalFunctionWriter>(_context, this, &_externalFunction);
}

ExternalFunctionWriter::ExternalFunctionWriter(Context* context,
                                               SinkOperator* sinkOperator,
                                               ExternalFunction* externalFunction)
    : SinkWriter(context, sinkOperator), _externalFunction{externalFunction} {}

void ExternalFunctionWriter::connect() {
    tassert(992601, "isSink should be true for sink stage", _externalFunction->getOptions().isSink);
    _externalFunction->validateFunctionConnection();
}

OperatorStats ExternalFunctionWriter::processDataMsg(StreamDataMsg dataMsg) {
    OperatorStats stats;

    if (dataMsg.docs.empty()) {
        return stats;
    }

    _externalFunction->updateRateLimitPerSecond(
        getExternalFunctionRateLimitPerSec(_context->featureFlags));

    bool samplersPresent = samplersExist();
    for (auto& streamDoc : dataMsg.docs) {
        auto result = _externalFunction->processStreamDoc(&streamDoc);
        stats.numInputBytes += result.numInputBytes;
        stats.numOutputBytes += result.numOutputBytes;
        stats.numDlqDocs += result.numDlqDocs;
        stats.numDlqBytes += result.numDlqBytes;
        if (result.numDlqDocs == 0) {
            stats.numOutputDocs++;
            if (samplersPresent) {
                StreamDataMsg msg;
                msg.docs.push_back(std::move(streamDoc));
                sendOutputToSamplers(std::move(msg));
            }
        }
    }

    stats.timeSpent = dataMsg.creationTimer->elapsed();

    return stats;
}

size_t ExternalFunctionWriter::partition(StreamDocument& doc) {
    // This needs to be thought through with ordering when multi-parallelism is supported.
    return mongo::ValueComparator::kInstance.hash(Value{OID::gen()});
}

}  // namespace streams
//
