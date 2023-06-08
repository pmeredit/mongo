#pragma once

#include "streams/exec/document_timestamp_extractor.h"
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
        Context* context{nullptr};
        // May be nullptr.
        DocumentTimestampExtractor* timestampExtractor{nullptr};
        // The output field name for the event timestamp. Must be set.
        // TODO SERVER-77563: This may not work correctly for dotted paths.
        std::string timestampOutputFieldName;
    };

    SourceOperator(int32_t numInputs, int32_t numOutputs) : Operator(numInputs, numOutputs) {}

    virtual ~SourceOperator() = default;

    // Reads a batch of documents from the source and sends them through the OperatorDag.
    // Returns the number of documents read from the source in this run.
    int32_t runOnce();

protected:
    virtual int32_t doRunOnce() = 0;

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override {
        MONGO_UNREACHABLE;
    }
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        MONGO_UNREACHABLE;
    }
};

}  // namespace streams
