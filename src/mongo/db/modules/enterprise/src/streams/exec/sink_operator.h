#pragma once

#include "mongo/util/intrusive_counter.h"
#include "streams/exec/operator.h"
#include "streams/exec/output_sampler.h"

namespace streams {

class OutputSampler;
struct Context;

/**
 * The base class of all sink operators.
 */
class SinkOperator : public Operator {
public:
    SinkOperator(Context* context, int32_t numInputs)
        : Operator(context, numInputs, /*numOutputs*/ 0) {}

    virtual ~SinkOperator() = default;

    void addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler);

protected:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) final;

    // This is called by doOnDataMsg() to write the documents to the sink.
    virtual void doSinkOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) = 0;

    bool shouldComputeInputByteStats() const override {
        return true;
    }

    void sendOutputToSamplers(const StreamDataMsg& dataMsg);

    std::vector<boost::intrusive_ptr<OutputSampler>> _outputSamplers;
};

}  // namespace streams
