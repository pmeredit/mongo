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
    };

    SourceOperator(Context* context, int32_t numOutputs);

    virtual ~SourceOperator() = default;

    // Attempts connection with the input source. Does nothing if the connection is already
    // established. This should be called before start() and should be called repeatedly until
    // getConnectionStatus() returns a kConnected or kError status.
    void connect() {
        doConnect();
    }

    // Whether this SourceOperator is connected to the input source.
    ConnectionStatus getConnectionStatus() {
        return doGetConnectionStatus();
    }

    // Reads a batch of documents from the source and sends them through the OperatorDag.
    // Returns the number of documents read from the source in this run.
    int64_t runOnce();

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
    virtual void doConnect() {}
    virtual ConnectionStatus doGetConnectionStatus() {
        return ConnectionStatus{ConnectionStatus::Status::kConnected};
    }

    virtual void doIncOperatorStats(OperatorStats stats) final;

    // _lastControlMsg is updated whenever the source instance sends a watermark message.
    StreamControlMsg _lastControlMsg;
};

}  // namespace streams
