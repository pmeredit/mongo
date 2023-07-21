#pragma once

#include "mongo/util/periodic_runner.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

class Executor;
class CheckpointStorage;

using namespace mongo;

/**
 * CheckpointCoordinator is responsible for starting checkpoints.
 * The implementation currently uses a PeriodicJob thread to inject
 * checkpoint messages into the Executor.
 * The ideal checkpoint frequency is affected by the type of pipeline.
 * There are currently three pipeline types to consider:
 * 1. Pipelines without windows.
 *      Checkpoints are inexpensive.
 * 2. Pipelines with windows, using closed window checkpointing.
 *      Checkpoints are inexpensive.
 * 3. Pipelines with windows, using open window checkpoint.
 *      Checkpoints are expensive. Only 1 active checkpoint allowed at once.
 * CheckpointCoordinator is currently implemented with (1) and (2) in mind, and
 * will need some changes to support (3) in SERVER-77128.
 */
class CheckpointCoordinator {
public:
    // kShortInterval is a 5 minute interval intended for pipelines with inexpensive checkpoints.
    // This should include pipelines without windows and pipelines using the
    // "closed window checkpointing" approach.
    constexpr static mongo::Milliseconds kFiveMinuteInterval{1000 * 60 * 5};

    struct Options {
        // This name is used to identify the background job.
        std::string processorId;
        // A checkpoint message is created every "interval" milliseconds.
        mongo::Milliseconds interval{kFiveMinuteInterval};
        // A checkpoint message is injected into this executor.
        Executor* executor{nullptr};
        // Used to create a checkpoint ID.
        CheckpointStorage* storage{nullptr};
        // Service context for running the background job.
        mongo::ServiceContext* svcCtx{nullptr};
    };

    CheckpointCoordinator(Options options);

    ~CheckpointCoordinator();

private:
    void startCheckpoint();

    Options _options;
    // Background periodic job that kicks off checkpoints.
    mongo::PeriodicJobAnchor _backgroundjob;
};

}  // namespace streams
