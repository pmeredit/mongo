/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/log_sink_operator.h"

#include "mongo/logv2/log.h"
#include "streams/exec/context.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

void LogSinkOperator::doSinkOnDataMsg(int32_t inputIdx,
                                      StreamDataMsg dataMsg,
                                      boost::optional<StreamControlMsg> controlMsg) {
    for (auto& doc : dataMsg.docs) {
        LOGV2_INFO(5739600, "data", "doc"_attr = doc.doc.toString());
    }

    if (controlMsg) {
        logControl(controlMsg.value());
    }
}

void LogSinkOperator::doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    logControl(controlMsg);
}

void LogSinkOperator::logControl(StreamControlMsg controlMsg) {
    if (controlMsg.watermarkMsg) {
        int64_t watermarkTime = controlMsg.watermarkMsg->eventTimeWatermarkMs;
        auto watermarkStatus = controlMsg.watermarkMsg->watermarkStatus;
        LOGV2_INFO(5739601,
                   "watermarkMsg",
                   "name"_attr = _context->streamName,
                   "streamProcessorId"_attr = _context->streamProcessorId,
                   "watermarkTime"_attr = watermarkTime,
                   "watermarkStatus"_attr = watermarkStatus);
    } else if (controlMsg.checkpointMsg) {
        LOGV2_INFO(5739602,
                   "checkpointMsg",
                   "name"_attr = _context->streamName,
                   "streamProcessorId"_attr = _context->streamProcessorId,
                   "checkpointId"_attr = controlMsg.checkpointMsg->id);
    } else {
        LOGV2_INFO(5739603,
                   "eofSignal",
                   "name"_attr = _context->streamName,
                   "streamProcessorId"_attr = _context->streamProcessorId);
    }
}

}  // namespace streams
