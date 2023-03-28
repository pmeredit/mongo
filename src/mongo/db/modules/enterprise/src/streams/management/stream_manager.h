/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#pragma once

#include "mongo/platform/mutex.h"
#include "streams/commands/start_stream_processor_gen.h"
#include "streams/exec/operator_dag.h"
#include <memory>

namespace streams {

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

    // Note: currently only used in testing.
    std::unique_ptr<OperatorDag>& getStreamProcessorInfo(const std::string& name) {
        return _processors.at(name);
    }

private:
    // The mutex that protects calls to startStreamProcessor.
    mongo::Mutex _mutex = MONGO_MAKE_LATCH("StreamManager::_mutex");

    // The map of streamProcessors.
    std::map<std::string, std::unique_ptr<OperatorDag>> _processors;
};

}  // namespace streams
