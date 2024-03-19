#pragma once

#include "streams/exec/connection_status.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/watermark_generator.h"

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
        // The output field name for the event timestamp. Must be set.
        std::string timestampOutputFieldName;
        // If true, watermarks are created and sent in this $source.
        bool useWatermarks{false};
        // Allowed lateness specified in the $source.
        int64_t allowedLatenessMs{0};
        // If true, kIdle watermark messages are sent whenever 0 documents are returned
        // from the source.
        bool sendIdleMessages{false};
        // Max number of bytes to prefetch.
        int64_t maxPrefetchByteSize{kDataMsgMaxByteSize * 10};
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

    // Called by the Executor when a checkpoint is committed.
    void onCheckpointCommit(CheckpointId checkpointId) {
        doOnCheckpointCommit(checkpointId);
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

    virtual void doOnCheckpointCommit(CheckpointId checkpointId) {
        // No-op by default.
    }

    void doIncOperatorStats(OperatorStats stats) final;

    virtual boost::optional<mongo::BSONObj> doGetRestoredState() {
        return boost::none;
    }
    virtual boost::optional<mongo::BSONObj> doGetLastCommittedState() {
        return boost::none;
    }

    // _lastControlMsg is updated whenever the source instance sends a watermark message.
    StreamControlMsg _lastControlMsg;
};

}  // namespace streams
