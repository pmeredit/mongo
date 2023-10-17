#pragma once

#include <memory>
#include <queue>

#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/future.h"
#include "mongo/util/producer_consumer_queue.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"
#include "streams/util/metrics.h"

namespace streams {

class CheckpointCoordinator;
class DeadLetterQueue;
class OperatorDag;
class OutputSampler;
struct Context;

/**
 * This class executes an OperatorDag. The thread in this class is the one on which all
 * the data flow between Operators occur. This class is not thread-safe.
 */
class Executor {
public:
    struct Options {
        OperatorDag* operatorDag{nullptr};
        CheckpointCoordinator* checkpointCoordinator{nullptr};
        // Sleep duration when source is idle.
        int32_t sourceIdleSleepDurationMs{2000};
        // Sleep duration when source is not idle.
        // This is currently always zero except when a sample data source is used.
        int32_t sourceNotIdleSleepDurationMs{0};
        // Whether the executor should send one last CheckpointControlMsg through the OperatorDag
        // before shutting down.
        bool sendCheckpointControlMsgBeforeShutdown{true};
        // Initial connection fails if it takes longer than this.
        mongo::Seconds connectTimeout{60};
        // Max amount of bytes that can be buffered in the in-memory buffer. Once
        // this threshold is hit, all subsequent inserts will block until there is
        // space available.
        int64_t testOnlyDocsQueueMaxSizeBytes{512 * 1024 * 1024};  // 512 MB
    };

    Executor(Context* context, Options options);

    ~Executor();

    // Starts the OperatorDag and _executorThread.
    // Returns a Future that would be completed with an error when the stream processor runs into
    // an error.
    mongo::Future<void> start();

    // Stops the OperatorDag and _executorThread.
    void stop();

    // True if the Operators have succesfully connected and started.
    bool isStarted();

    // Returns stats for each operator.
    std::vector<OperatorStats> getOperatorStats();

    // Adds an OutputSampler to register with the SinkOperator.
    void addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler);

    // Test-only method to insert documents into a stream that uses InMemorySourceOperator as the
    // source.
    void testOnlyInsertDocuments(std::vector<mongo::BSONObj> docs);

    // Test-only method to inject an exception into runLoop().
    void testOnlyInjectException(std::exception_ptr exception);

private:
    friend class CheckpointTestWorkload;
    friend class CheckpointTest;
    friend class StreamManagerTest;

    enum class RunStatus {
        kActive,
        kIdle,
        kShutdown,
    };

    // Cost function for the bounded test only docs MPSC queue.
    struct TestOnlyDocsQueueCostFunc {
        size_t operator()(const std::vector<mongo::BSONObj>& objs) const {
            size_t out{0};
            for (const auto& obj : objs) {
                out += obj.objsize();
            }
            return out;
        }
    };

    // Called repeatedly by runLoop() to do the actual work.
    // Returns the number of documents read from the source in this run.
    RunStatus runOnce();

    // _executorThread uses this to continuously read documents from the source operator of the
    // OperatorDag and get them sent through the OperatorDag.
    void runLoop();

    // Sends the given CheckpointControlMsg through the OperatorDag.
    void sendCheckpointControlMsg(CheckpointControlMsg msg);

    // Call connect until operators have connected.
    // This method is called once at the beginning of the Executor's background thread.
    void connect(mongo::Date_t deadline);

    // Takes the mutex and checks for _shutdown.
    bool isShutdown();

    // Updates the stream stats snapshot stored on the executor and also updates the corresponding
    // metrics for those stats.
    void updateStats(StreamStats stats);

    // Context of the streamProcessor, used for logging purposes.
    Context* _context{nullptr};
    Options _options;
    mongo::Promise<void> _promise;
    mongo::stdx::thread _executorThread;
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("Executor::mutex");
    bool _shutdown{false};
    bool _started{false};
    StreamStats _streamStats;
    std::vector<boost::intrusive_ptr<OutputSampler>> _outputSamplers;
    boost::optional<std::exception_ptr> _testOnlyException;

    // Documents are inserted into this queue from `testOnlyInsert` potentially be multiple
    // producers, and are only consumed by the single threaded executor run loop.
    mongo::MultiProducerSingleConsumerQueue<std::vector<mongo::BSONObj>, TestOnlyDocsQueueCostFunc>
        _testOnlyDocsQueue;

    // Set of metrics that are periodically updated in the run loop when operator
    // stats are polled.
    std::shared_ptr<Counter> _numInputDocumentsCounter;
    std::shared_ptr<Counter> _numInputBytesCounter;
    std::shared_ptr<Counter> _numOutputDocumentsCounter;
    std::shared_ptr<Counter> _numOutputBytesCounter;
};

};  // namespace streams
