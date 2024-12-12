/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/message.h"
#include <fmt/format.h>

#include "streams/util/metric_manager.h"

namespace streams {

struct Context;

// A convenience macro to assert during checkpoint read operations.
// TODO(SERVER-78501): Add specific error codes in checkpoint assertions.
#define CHECKPOINT_RECOVERY_ASSERT(checkpointId, operatorId, msg, assertion)                     \
    uassert(ErrorCodes::InternalError,                                                           \
            fmt::format("checkpointId: {} operatorId: {} encountered error during recovery: {}", \
                        checkpointId,                                                            \
                        operatorId,                                                              \
                        msg),                                                                    \
            assertion);

// A convenience macro to assert during checkpoint write operations.
#define CHECKPOINT_WRITE_ASSERT(checkpointId, operatorId, msg, assertion)                     \
    uassert(ErrorCodes::InternalError,                                                        \
            fmt::format("checkpointId: {} operatorId: {} encountered error during write: {}", \
                        checkpointId,                                                         \
                        operatorId,                                                           \
                        msg),                                                                 \
            assertion);

// Get default labels for a specific streamProcessor's metrics.
MetricManager::LabelsVec getDefaultMetricLabels(Context* context);

// Allows StreamDataMsg to be use in LOGV2 statements.
mongo::BSONObj toBSON(const StreamDataMsg& msg);

// Allows StreamControlMsg to be use in LOGV2 statements.
mongo::BSONObj toBSON(const StreamControlMsg& msg);

// The reason a stop was requested (used for logging).
enum class StopReason {
    // An external stop request from the streams agent.
    ExternalStopRequest,
    // Stopping due to pod shutdown.
    Shutdown,
    // Stopping due to an error during start.
    ErrorDuringStart,
    // An external start request for a stream processor currently in a failed state.
    ExternalStartRequestForFailedState,
    // This is just a validation request that has finished
    DoneValidating
};

// Used for printing a stopReason in LOGV2 statements.
std::string stopReasonToString(StopReason stopReason);

}  // namespace streams
