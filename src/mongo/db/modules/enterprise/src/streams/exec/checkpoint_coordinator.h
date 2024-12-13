/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <chrono>

#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/concurrent_checkpoint_monitor.h"
#include "streams/exec/message.h"
#include "streams/util/units.h"

using mongo::stdx::chrono::system_clock;

namespace mongo {
class ServiceContext;
}

namespace streams {

class Executor;
class CheckpointStorage;

enum class WriteCheckpointCommand { kNone, kNormal, kForce };

/**
 * CheckpointCoordinator determines when a CheckpointControlMsg should be sent through the
 * OperatorDag to initiate a checkpoint.
 */
class CheckpointCoordinator {
public:
    struct Options {
        // This name is used to identify the background job.
        std::string processorId;
        // Whether dataflow is enabled. If not, CheckpointCoordinator decides to not send any
        // CheckpointControlMsgs through the OperatorDag.
        bool enableDataFlow{true};
        // If we don't have a restore checkpoint, we want to write a checkpoint
        // before we start executing. We do this to have a well defined starting point
        // so if a crash occurs after data is output, we can get back to the same input data.
        bool writeFirstCheckpoint{false};
        // Minimum periodic checkpoint interval.
        mongo::Milliseconds minInterval{mongo::Minutes(5)};
        // Maximum periodic checkpoint interval.
        mongo::Milliseconds maxInterval{mongo::Minutes(60)};
        // The state size at which the maximum interval will be used.
        int64_t stateSizeToUseMaxInterval{100_MiB};
        // A fixed interval to use between checkpoints.
        boost::optional<mongo::Milliseconds> fixedInterval;
        // The checkpoint storage.
        CheckpointStorage* storage{nullptr};
        std::shared_ptr<ConcurrentCheckpointController> checkpointController;
        // Commit timestamp of the restored checkpoint
        boost::optional<mongo::stdx::chrono::time_point<system_clock>> restoredCheckpointTimestamp;
    };

    struct CheckpointRequest {
        // If input source is a mongo changestream operator and we have a newer resume token.
        bool changeStreamAdvanced{false};
        // Does an operator have uncheckpointed state since the last commit? This
        // currently is determined based on whether there are new output or dlq docs
        bool uncheckpointedState{false};
        // Do we have an externally requested checkpoint request?
        WriteCheckpointCommand writeCheckpointCommand{WriteCheckpointCommand::kNone};
        // Are we shutting down?
        bool shutdown{false};
        // Size of the last committed checkpoint in bytes
        int64_t lastCheckpointSizeBytes{0};
    };

    CheckpointCoordinator(Options options);

    // Possibly returns a CheckpointControlMsg to send through the OperatorDag.
    // If this function yields a CheckpointControlMsg, then that message cannot
    // be discarded and it _must_ be injected into the pipeline. Failure to do so
    // will trigger an assert the next time a checkpoint needs to be taken as well as potentially
    // block other processors from taking checkpoints
    boost::optional<CheckpointControlMsg> getCheckpointControlMsgIfReady(
        const CheckpointRequest& req);

    bool writtenFirstCheckpoint() const {
        return _writtenFirstCheckpoint;
    }

    void setCheckpointInterval(mongo::Milliseconds value) {
        _options.fixedInterval = value;
        _interval = value;
    }

private:
    friend class CheckpointTest;
    friend class StreamManagerTest;

    mongo::Milliseconds getDynamicInterval(int64_t stateSize);

    enum class CreateCheckpoint { kNotNeeded, kIfRoom, kForce };
    CreateCheckpoint evaluateIfCheckpointShouldBeWritten(const CheckpointRequest& req);
    CheckpointControlMsg createCheckpointControlMsg();
    bool _writtenFirstCheckpoint{false};

    Options _options;
    mongo::stdx::chrono::time_point<system_clock> _lastCheckpointTimestamp;
    mongo::Milliseconds _interval;
};

}  // namespace streams
