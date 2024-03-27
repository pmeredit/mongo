#pragma once

#include <chrono>

#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/message.h"

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
        // Determines the frequency at which checkpoint messages are created.
        mongo::stdx::chrono::milliseconds checkpointIntervalMs;
        // Operator info, like stats, in the restore checkpoint.
        boost::optional<std::vector<mongo::CheckpointOperatorInfo>> restoreCheckpointOperatorInfo;
        // The checkpoint storage.
        CheckpointStorage* storage{nullptr};
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
    };

    CheckpointCoordinator(Options options);

    // Possibly returns a CheckpointControlMsg to send through the OperatorDag.
    // If this function yields a CheckpointControlMsg, then that message cannot
    // be discarded and it _must_ be injected into the pipeline. Failure to do so
    // will trigger an assert the next time a checkpoint needs to be taken
    boost::optional<CheckpointControlMsg> getCheckpointControlMsgIfReady(
        const CheckpointRequest& req);

    // Return the checkpoint interval.
    const mongo::stdx::chrono::milliseconds& getCheckpointInterval() {
        return _options.checkpointIntervalMs;
    }

    bool writtenFirstCheckpoint() const {
        return _writtenFirstCheckpoint;
    }

    void setCheckpointInterval(mongo::stdx::chrono::milliseconds value) {
        _options.checkpointIntervalMs = value;
    }

private:
    CheckpointControlMsg createCheckpointControlMsg();
    bool _writtenFirstCheckpoint{false};

    Options _options;
    mongo::stdx::chrono::time_point<mongo::stdx::chrono::steady_clock> _lastCheckpointTimestamp;
};

}  // namespace streams
