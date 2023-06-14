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
        // May be nullptr.
        DocumentTimestampExtractor* timestampExtractor{nullptr};
        // The output field name for the event timestamp. Must be set.
        // TODO SERVER-77563: This may not work correctly for dotted paths.
        std::string timestampOutputFieldName;
        // If true, watermarks are created and sent in this $source.
        bool useWatermarks{false};
        // Allowed lateness specified in the $source.
        int64_t allowedLatenessMs{0};
    };

    SourceOperator(Context* context, int32_t numOutputs)
        : Operator(context, /*numInputs*/ 0, numOutputs) {}

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
