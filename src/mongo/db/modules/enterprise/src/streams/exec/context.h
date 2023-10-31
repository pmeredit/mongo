#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/expression_context.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/old_checkpoint_storage.h"
#include "streams/exec/stages_gen.h"

namespace streams {

class MetricManager;

// Encapsulates the top-level state of a stream processor.
struct Context {
    std::string tenantId;
    std::string streamName;
    std::string streamProcessorId;
    mongo::stdx::unordered_map<std::string, mongo::Connection> connections;
    std::string clientName;
    mongo::ServiceContext::UniqueClient client;
    mongo::ServiceContext::UniqueOperationContext opCtx;
    boost::intrusive_ptr<mongo::ExpressionContext> expCtx;
    MetricManager* metricManager{nullptr};
    // Dead letter queue to which documents that could not be processed are added.
    std::unique_ptr<DeadLetterQueue> dlq;
    bool isEphemeral{false};
    // Checkpoint storage. When checkpointing is not enabled, may be nullptr.
    std::unique_ptr<OldCheckpointStorage> oldCheckpointStorage;
    // The CheckpointId the streamProcessor was restored from.
    boost::optional<CheckpointId> restoreCheckpointId;

    // The new checkpoint storage interface. This is currently only set in unit tests.
    std::unique_ptr<CheckpointStorage> checkpointStorage;

    mongo::BSONObj toBSON();
};

}  // namespace streams
