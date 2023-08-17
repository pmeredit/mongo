#pragma once

#include <chrono>

#include "mongo/util/periodic_runner.h"
#include "streams/exec/message.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

class Executor;
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
        CheckpointStorage* storage{nullptr};
        // If we don't have a restore checkpoint, we want to write a checkpoint
        // before we start executing. We do this to have a well defined starting point
        // so if a crash occurs after data is output, we can get back to the same input data.
        bool writeFirstCheckpoint{false};
        // Determines the frequency at which checkpoint messages are created.
        // The ideal checkpoint frequency is affected by the type of pipeline.
        // There are currently three pipeline types to consider:
        // 1. Pipelines without windows.
        //    Checkpoints are inexpensive.
        // 2. Pipelines with windows, using closed window checkpointing.
        //    Checkpoints are inexpensive.
        // 3. Pipelines with windows, using open window checkpoint.
        //    Checkpoints are expensive. Only 1 active checkpoint allowed at once.
        mongo::stdx::chrono::milliseconds checkpointIntervalMs{5 * 60 * 1000};
    };

    CheckpointCoordinator(Options options);

    // Returns a CheckpointControlMsg to send through the OperatorDag if it's time to write a new
    // checkpoint. Returns boost::none otherwise.
    boost::optional<CheckpointControlMsg> getCheckpointControlMsgIfReady(bool force = false);

private:
    CheckpointControlMsg createCheckpointControlMsg();

    Options _options;
    mongo::stdx::chrono::time_point<mongo::stdx::chrono::steady_clock> _lastCheckpointTimestamp;
};

}  // namespace streams
