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
#include "streams/exec/output_sampler.h"

namespace streams {

class MetricManager;

// Encapsulates the top-level state of a stream processor.
struct Context {
    MetricManager* metricManager{nullptr};
    std::string tenantId;
    std::string streamName;
    std::string streamProcessorId;
    std::string clientName;
    mongo::ServiceContext::UniqueClient client;
    mongo::ServiceContext::UniqueOperationContext opCtx;
    boost::intrusive_ptr<mongo::ExpressionContext> expCtx;
    // Dead letter queue to which documents that could not be processed are added.
    std::unique_ptr<DeadLetterQueue> dlq;
    bool isEphemeral{false};
    // Checkpoint storage. When checkpointing is not enabled, may be nullptr.
    std::unique_ptr<CheckpointStorage> checkpointStorage;
};

}  // namespace streams
