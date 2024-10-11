/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/stdx/mutex.h"

namespace streams {

// Responsible for enforcing a max concurrency for checkpoints in progress.
class ConcurrentCheckpointController {
public:
    ConcurrentCheckpointController(int32_t maxConcurrentCheckpoints);

    // Returns true if checkpoint lock has been acquired
    bool startNewCheckpointIfRoom(bool force);

    void onCheckpointComplete();

    void setMaxInprogressCheckpoints(int32_t maxConcurrentCheckpoints);

private:
    // Max amount of in progress checkpoints allowed to be used across all stream processors. Once
    // this limit is exceeded, will return false for starting new checkpoints.
    int32_t _maxConcurrentCheckpoints{0};

    int32_t _concurrentCheckpoints{0};

    // The mutex that protects changes to the InProgressCheckpoionts.
    mongo::stdx::mutex _mutex;
};  // class ConcurrentCheckpointController

};  // namespace streams
