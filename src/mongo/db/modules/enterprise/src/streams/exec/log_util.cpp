/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/log_util.h"

#include <string>
#include <vector>

#include "mongo/logv2/log.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

// Get default labels for a specific streamProcessor's metrics.
Metric::LabelsVec getDefaultMetricLabels(Context* context) {
    Metric::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, context->streamProcessorId));
    labels.push_back(std::make_pair(kProcessorNameLabelKey, context->streamName));
    return labels;
}

mongo::BSONObj toBSON(const StreamDataMsg& msg) {
    return msg.toBSONForLogging();
}

mongo::BSONObj toBSON(const StreamControlMsg& msg) {
    return msg.toBSONForLogging();
}

std::string stopReasonToString(StopReason stopReason) {
    switch (stopReason) {
        case StopReason::ExternalStopRequest:
            return "ExternalStopRequest";
        case StopReason::Shutdown:
            return "Shutdown";
        case StopReason::ErrorDuringStart:
            return "ErrorDuringStart";
        case StopReason::ExternalStartRequestForFailedState:
            return "ExternalStartRequestForFailedState";
        default:
            return "Unknown";
    }
}

mongo::BSONObj toBSON(const LoggingContext& loggingContext) {
    return BSON("streamProcessorName" << loggingContext.streamProcessorName << "streamProcessorId"
                                      << loggingContext.streamProcessorId << "tenantId"
                                      << loggingContext.tenantId);
}

}  // namespace streams
