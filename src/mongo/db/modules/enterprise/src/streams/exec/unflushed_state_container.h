#pragma once

#include "streams/exec/message.h"

namespace streams {

// UnflushedStateContainer is used by CheckpointStorage and SourceOperator classes to hold
// information about checkpoints that have not yet been flushed to remote storage.
//
// Later, when the checkpoint is flushed to remote storage, this class is used to retrieve that
// information. This is helpful to implement things like committing Kafka consumer group offsets
// only after the corresponding checkpoint is flushed to remote storage.
class UnflushedStateContainer {
public:
    // Add an unflushed checkpointID and associated BSONObj.
    void add(CheckpointId id, mongo::BSONObj state);

    // pop returns the information about a checkpointId and removes it from the internal state.
    // It should only be called on the oldest unflushed checkpointId.
    mongo::BSONObj pop(CheckpointId checkpointId);

private:
    std::deque<std::pair<CheckpointId, mongo::BSONObj>> _unflushedCheckpoints;
};

}  // namespace streams
