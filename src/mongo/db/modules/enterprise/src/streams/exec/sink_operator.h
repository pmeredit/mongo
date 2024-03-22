#pragma once

#include <boost/optional.hpp>
#include <string>

#include "mongo/util/intrusive_counter.h"
#include "streams/exec/connection_status.h"
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
    SinkOperator(Context* context, int32_t numInputs);

    ~SinkOperator() override = default;

    void addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler);

    // Flush any remaining messages to the target sink. This is called before
    // the checkpoint is committed.
    void flush();

    /**
     * Returns whether this Operator is connected to its remote target, if it has one.
     */
    ConnectionStatus getConnectionStatus() {
        return doGetConnectionStatus();
    }

protected:
    virtual ConnectionStatus doGetConnectionStatus() {
        return ConnectionStatus{ConnectionStatus::Status::kConnected};
    }

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) final;

    // This currently commits checkpoints and calls doSinkOnControlMsg for sink
    // specific behavior.
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) final;

    // This is called by doOnDataMsg() to write the documents to the sink.
    virtual void doSinkOnDataMsg(int32_t inputIdx,
                                 StreamDataMsg dataMsg,
                                 boost::optional<StreamControlMsg> controlMsg) = 0;

    // This is called by doOnControlMsg() for any sink specific control message behavior.
    virtual void doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {}

    void doIncOperatorStats(OperatorStats stats) final;

    // The derived class must flush all documents to the actual sink before this call returns.
    virtual void doFlush() {}

    bool shouldComputeInputByteStats() const override {
        return true;
    }

    bool doIsSink() final {
        return true;
    }

    bool samplersExist() const;

    void sendOutputToSamplers(const StreamDataMsg& dataMsg);

    std::vector<boost::intrusive_ptr<OutputSampler>> _outputSamplers;

private:
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("SinkOperator::mutex");
};

}  // namespace streams
