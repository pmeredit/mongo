#pragma once

#include <deque>
#include <memory>

#include "mongo/platform/mutex.h"
#include "mongo/stdx/thread.h"

namespace streams {

class OperatorDag;
class OutputSampler;

/**
 * This class executes an OperatorDag. The thread in this class is the one on which all
 * the data flow between Operators occur. This class is not thread-safe.
 */
class Executor {
public:
    struct Options {
        // Name of the stream procesor. Used for logging purposes.
        std::string streamProcessorName;
        OperatorDag* operatorDag{nullptr};
        // Sleep duration when source is idle.
        int32_t sourceIdleSleepDurationMs{2000};
    };

    Executor(Options options);

    ~Executor();

    // Starts the OperatorDag and _executorThread.
    void start();

    // Stops the OperatorDag and _executorThread.
    void stop();

    // Adds an OutputSampler to register with the SinkOperator.
    void addOutputSampler(OutputSampler* sampler);

private:
    // Called repeatedly by runLoop() to do the actual work.
    // Returns the number of documents read from the source in this run.
    int32_t runOnce();

    // _executorThread uses this to continuously read documents from the source operator of the
    // OperatorDag and get them sent through the OperatorDag.
    void runLoop();

    Options _options;
    mongo::stdx::thread _executorThread;
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("Executor::mutex");
    bool _shutdown{false};
    std::vector<OutputSampler*> _outputSamplers;
};

};  // namespace streams
