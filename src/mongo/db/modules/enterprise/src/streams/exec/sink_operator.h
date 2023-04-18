#pragma once

#include "mongo/util/intrusive_counter.h"
#include "streams/exec/operator.h"
#include "streams/exec/output_sampler.h"

namespace streams {

class OutputSampler;

/**
 * The base class of all sink operators.
 */
class SinkOperator : public Operator {
public:
    SinkOperator(int32_t numInputs) : Operator(numInputs, /*numOutputs*/ 0) {}

    virtual ~SinkOperator() = default;

    void addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler);

protected:
    void sendOutputToSamplers(const StreamDataMsg& dataMsg);

    std::vector<boost::intrusive_ptr<OutputSampler>> _outputSamplers;
};

}  // namespace streams
