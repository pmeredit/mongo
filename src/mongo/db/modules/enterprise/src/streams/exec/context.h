/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/concurrent_checkpoint_monitor.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/restored_checkpoint_info.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/stream_processor_feature_flags.h"

namespace streams {

class MetricManager;
class SourceBufferManager;

// Encapsulates the top-level state of a stream processor.
struct Context {
    std::string tenantId;
    std::string streamName;
    boost::optional<std::string> instanceName;
    std::string streamProcessorId;
    boost::optional<std::string> kafkaConsumerGroup;
    mongo::stdx::unordered_map<std::string, mongo::Connection> connections;
    std::string clientName;
    mongo::ServiceContext::UniqueClient client;
    mongo::ServiceContext::UniqueOperationContext opCtx;
    boost::intrusive_ptr<mongo::ExpressionContext> expCtx;
    // Dead letter queue to which documents that could not be processed are added.
    std::unique_ptr<DeadLetterQueue> dlq;
    bool isEphemeral{false};
    // Memory aggregator that tracks the memory usage for this specific stream processor.
    std::shared_ptr<mongo::ChunkedMemoryAggregator> memoryAggregator;

    std::shared_ptr<SourceBufferManager> sourceBufferManager;

    // The CheckpointId the streamProcessor was restored from.
    boost::optional<CheckpointId> restoreCheckpointId;

    // Pipeline version specified in the start request.
    int pipelineVersion{0};

    // Information about the restored checkpoint. Populated in CheckpointStorage
    // when startCheckpointRestore is called.
    boost::optional<RestoredCheckpointInfo> restoredCheckpointInfo;

    // The optimized execution plan used for this execution.
    std::vector<mongo::BSONObj> executionPlan;

    // The new checkpoint storage interface. This is currently only set in unit tests.
    std::unique_ptr<CheckpointStorage> checkpointStorage;

    // A thread-safe concurrency controller for tracking how many inprogress checkpoints are
    // currently active to prevent checkpoint coordinators from exceeding the defined concurrency
    // when applicable
    std::shared_ptr<ConcurrentCheckpointController> concurrentCheckpointController;
    // Stream processor feature flags.
    boost::optional<StreamProcessorFeatureFlags> featureFlags;
    // The stream metadata field name. If none, disable projecting stream metadata.
    boost::optional<std::string> streamMetaFieldName{boost::none};

    // If true, metadata is projected into user documents before the sink stage.
    bool projectStreamMetaPriorToSinkStage{false};
    // If true, stream metadata is projected into the _stream_meta field of the user document.
    bool projectStreamMeta{true};

    // During the start() of the SP, the source operator will try to honor this timeout.
    mongo::Seconds connectTimeout{60};

    // Set to true when starting a modified stream processor.
    bool isModifiedProcessor{false};

    // Set to true if the user's pipeline uses the meta expression to read stream
    // metadata, i.e. {$meta: "stream"}
    bool shouldUseDocumentMetadataFields{false};

    mongo::BSONObj toBSON() const;

    // For non sink stages, add metadata when there is explicit dependency of metadata in the
    // pipeline.
    bool shouldProjectStreamMetaPriorToSinkStage() {
        return projectStreamMeta && streamMetaFieldName && projectStreamMetaPriorToSinkStage;
    }

    // For sink stages, add metadata when there is no dependency of metadata in the
    // pipeline but the user has requested the metadata.
    bool shouldProjectStreamMetaInSinkStage() {
        return projectStreamMeta && streamMetaFieldName && !projectStreamMetaPriorToSinkStage;
    }

    ~Context();
};

// This function allows Context* to be used in LOG statements.
mongo::BSONObj toBSON(Context* context);

}  // namespace streams
