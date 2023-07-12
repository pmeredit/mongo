#pragma once

// A convenience macro to assert during checkpoint operations.
// TODO(SERVER-78501): Add specific error codes in checkpoint assertions.
#define CHECKPOINT_RECOVERY_ASSERT(checkpointId, operatorId, msg, assertion)                   \
    uassert(ErrorCodes::InternalError,                                                         \
            str::stream() << "checkpointId: " << checkpointId << " operatorId: " << operatorId \
                          << " encountered error: " << msg,                                    \
            assertion);
