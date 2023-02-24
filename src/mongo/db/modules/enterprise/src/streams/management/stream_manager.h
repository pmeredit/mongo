/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/platform/mutex.h"
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
    void startStreamProcessor(const std::vector<mongo::BSONObj>& pipeline);
};

}  // namespace streams
