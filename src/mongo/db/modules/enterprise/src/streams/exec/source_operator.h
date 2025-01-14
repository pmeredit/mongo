/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/source_buffer_manager.h"

namespace streams {

struct Context;

/**
 * The base class of all source operators.
 */
class SourceOperator : public Operator {
public:
    // Struct containing options common to all source operators.
    struct Options {
        // May be nullptr.
        DocumentTimestampExtractor* timestampExtractor{nullptr};
        // If set, the SourceOperator will project the assigned timestamp into this
        // field name. Currently this defaults to "_ts".
        boost::optional<std::string> timestampOutputFieldName;
        // If true, watermarks are created and sent in this $source.
        bool useWatermarks{false};
        // If true, kIdle watermark messages are sent whenever 0 documents are returned
        // from the source.
        bool sendIdleMessages{false};
        // If true, data flow is enabled.
        bool enableDataFlow{true};
    };

    SourceOperator(Context* context, int32_t numOutputs);

    ~SourceOperator() override = default;

    // Whether this SourceOperator is connected to the input source.
    // We assume that this method is only called by the Executor thread.
    ConnectionStatus getConnectionStatus() {
        return doGetConnectionStatus();
    }

    // Reads a batch of documents from the source and sends them through the OperatorDag.
    // Returns the number of documents read from the source in this run.
    int64_t runOnce();

    // Returns the state of the $source in the restore checkpoint.
    // The $source state is typically starting point and watermark information.
    boost::optional<mongo::BSONObj> getRestoredState() {
        return doGetRestoredState();
    }

    // Returns the state of the $source in the last committed checkpoint.
    // The $source state is typically starting point and watermark information.
    boost::optional<mongo::BSONObj> getLastCommittedState() {
        return doGetLastCommittedState();
    }

    // Called by the Executor when a checkpoint is flushed to remote storage.
    mongo::BSONObj onCheckpointFlush(CheckpointId checkpointId) {
        return doOnCheckpointFlush(checkpointId);
    }

protected:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override {
        MONGO_UNREACHABLE;
    }
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        MONGO_UNREACHABLE;
    }

    virtual const Options& getOptions() const = 0;

    bool doIsSource() final {
        return true;
    }

    virtual int64_t doRunOnce() = 0;
    virtual ConnectionStatus doGetConnectionStatus() {
        return ConnectionStatus{ConnectionStatus::Status::kConnected};
    }

    virtual mongo::BSONObj doOnCheckpointFlush(CheckpointId checkpointId) {
        // No-op by default.
        return mongo::BSONObj{};
    }

    void doIncOperatorStats(OperatorStats stats) final;

    virtual boost::optional<mongo::BSONObj> doGetRestoredState() {
        return boost::none;
    }
    virtual boost::optional<mongo::BSONObj> doGetLastCommittedState() {
        return boost::none;
    }

    SourceBufferManager::SourceBufferHandle _sourceBufferHandle;

    // _lastControlMsg is updated whenever the source instance sends a watermark message.
    StreamControlMsg _lastControlMsg;
};

}  // namespace streams
