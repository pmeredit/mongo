/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#pragma once

#include <memory>

#include "mongo/platform/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/exec/context.h"

namespace mongo {
class Connection;
class StartStreamSampleCommand;
}  // namespace mongo

namespace streams {

class Executor;
class OperatorDag;

/**
 * StreamManager is the entrypoint for all streamProcessor management operations.
 */
class StreamManager {
public:
    struct Options {
        // The period interval at which the background thread wakes up.
        int32_t backgroundThreadPeriodSeconds{5 * 60};
        // Prune inactive OutputSamplers after they have been inactive for this long.
        int32_t pruneInactiveSamplersAfterSeconds{5 * 60};
    };

    // Encapsulates a batch of sampled output records.
    struct OutputSample {
        std::vector<mongo::BSONObj> outputDocs;
        // Whether the sample request is fulfilled and there are no more output records to return
        // for this sample/cursor id.
        bool doneSampling{false};
    };

    // Get a reference to the global StreamManager singleton.
    static StreamManager& get();

    StreamManager(Options options);

    ~StreamManager();

    // Starts a new stream processor.
    void startStreamProcessor(std::string name,
                              const std::vector<mongo::BSONObj>& pipeline,
                              const std::vector<mongo::Connection>& connections);

    // Stop a streamProcessor.
    void stopStreamProcessor(std::string name);

    // Starts a sample request for the given stream processor.
    // Returns the cursor id to use for this sample request in getMoreFromSample() calls.
    int64_t startSample(const mongo::StartStreamSampleCommand& request);

    // Returns the next batch of sampled output records for a sample created via startSample().
    // When OutputSample.doneSampling is true, the cursor is automatically closed, so the caller
    // should not make any more getMoreFromSample() calls for the a cursor.
    // Throws if the stream processor or the cursor is not found.
    OutputSample getMoreFromSample(std::string name, int64_t cursorId, int64_t batchSize);

    void testOnlyInsertDocuments(std::string name, std::vector<mongo::BSONObj> docs);

private:
    friend class StreamManagerTest;

    // Encapsulates metadata for an OutputSampler.
    struct OutputSamplerInfo {
        int64_t cursorId{0};
        boost::intrusive_ptr<OutputSampler> outputSampler;
    };

    // Encapsulates state for a stream processor.
    struct StreamProcessorInfo {
        std::unique_ptr<Context> context;
        std::unique_ptr<OperatorDag> operatorDag;
        std::unique_ptr<Executor> executor;
        // The list of active OutputSamplers created for the ongoing sample() requests.
        std::vector<OutputSamplerInfo> outputSamplers;
        // Last cursor id used for a sample request.
        int64_t lastCursorId{0};
    };

    StreamProcessorInfo& getStreamProcessorInfo(const std::string& name) {
        return _processors.at(name);
    }

    void backgroundLoop();

    // Prunes OutputSampler instances that haven't been polled by the client in over 5mins.
    void pruneOutputSamplers();

    Options _options;
    // The mutex that protects calls to startStreamProcessor.
    mongo::Mutex _mutex = MONGO_MAKE_LATCH("StreamManager::_mutex");
    bool _shutdown{false};
    // The map of streamProcessors.
    mongo::stdx::unordered_map<std::string, StreamProcessorInfo> _processors;
    // Background thread that performs any background operations like state pruning.
    mongo::stdx::thread _backgroundThread;
};

}  // namespace streams
