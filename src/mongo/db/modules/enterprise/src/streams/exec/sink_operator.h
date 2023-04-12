#pragma once

#include "streams/exec/operator.h"

namespace streams {

class OutputSampler;

/**
 * The base class of all sink operators.
 */
class SinkOperator : public Operator {
public:
    SinkOperator(int32_t numInputs) : Operator(numInputs, /*numOutputs*/ 0) {}

    virtual ~SinkOperator() = default;

    void addOutputSampler(OutputSampler* sampler);

protected:
    void sendOutputToSamplers(const StreamDataMsg& dataMsg);

    std::vector<OutputSampler*> _outputSamplers;
};

}  // namespace streams
