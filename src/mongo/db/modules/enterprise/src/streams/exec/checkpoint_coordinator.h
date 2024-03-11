#pragma once

#include <chrono>

#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/message.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

class Executor;
class OldCheckpointStorage;
class CheckpointStorage;

/**
 * CheckpointCoordinator determines when a CheckpointControlMsg should be sent through the
 * OperatorDag to initiate a checkpoint.
 */
class CheckpointCoordinator {
public:
    struct Options {
        // This name is used to identify the background job.
        std::string processorId;
        // Used to create a checkpoint ID.
        OldCheckpointStorage* oldStorage{nullptr};
        // If we don't have a restore checkpoint, we want to write a checkpoint
        // before we start executing. We do this to have a well defined starting point
        // so if a crash occurs after data is output, we can get back to the same input data.
        bool writeFirstCheckpoint{false};
        // Determines the frequency at which checkpoint messages are created.
        mongo::stdx::chrono::milliseconds checkpointIntervalMs;
        // Operator info, like stats, in the restore checkpoint.
        boost::optional<std::vector<mongo::CheckpointOperatorInfo>> restoreCheckpointOperatorInfo;
        // This is the new storage interface, currently only used in unit tests.
        CheckpointStorage* storage{nullptr};
    };

    CheckpointCoordinator(Options options);

    // Returns a CheckpointControlMsg to send through the OperatorDag if it's time to write a new
    // checkpoint. Returns boost::none otherwise.
    boost::optional<CheckpointControlMsg> getCheckpointControlMsgIfReady(bool force = false);

    // Return the checkpoint interval.
    const mongo::stdx::chrono::milliseconds& getCheckpointInterval() {
        return _options.checkpointIntervalMs;
    }

private:
    CheckpointControlMsg createCheckpointControlMsg();

    Options _options;
    mongo::stdx::chrono::time_point<mongo::stdx::chrono::steady_clock> _lastCheckpointTimestamp;
};

}  // namespace streams
