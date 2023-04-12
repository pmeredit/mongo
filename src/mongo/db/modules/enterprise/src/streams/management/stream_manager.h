/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#pragma once

#include "mongo/platform/mutex.h"
#include "streams/commands/start_stream_processor_gen.h"
#include <memory>

namespace streams {

class Executor;
class OperatorDag;

/**
 * StreamManager is the entrypoint for all streamProcessor management operations.
 */
class StreamManager {
public:
    // Get a reference to the global StreamManager singleton.
    static StreamManager& get();

    // Start a new streamProcessor.
    void startStreamProcessor(std::string name,
                              const std::vector<mongo::BSONObj>& pipeline,
                              const std::vector<mongo::Connection>& connections);

private:
    friend class StreamManagerTest;

    // Encapsulates state for a stream processor.
    struct StreamProcessorInfo {
        std::unique_ptr<OperatorDag> operatorDag;
        std::unique_ptr<Executor> executor;
    };

    StreamProcessorInfo& getStreamProcessorInfo(const std::string& name) {
        return _processors.at(name);
    }

    // The mutex that protects calls to startStreamProcessor.
    mongo::Mutex _mutex = MONGO_MAKE_LATCH("StreamManager::_mutex");

    // The map of streamProcessors.
    std::map<std::string, StreamProcessorInfo> _processors;
};

}  // namespace streams
