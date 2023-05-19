#pragma once

#include "streams/exec/operator.h"

namespace streams {

/**
 * The base class of all source operators.
 */
class SourceOperator : public Operator {
public:
    SourceOperator(int32_t numInputs, int32_t numOutputs) : Operator(numInputs, numOutputs) {}

    virtual ~SourceOperator() = default;

    // Reads a batch of documents from the source and sends them through the OperatorDag.
    // Returns the number of documents read from the source in this run.
    int32_t runOnce();

protected:
    virtual int32_t doRunOnce() = 0;
};

}  // namespace streams
