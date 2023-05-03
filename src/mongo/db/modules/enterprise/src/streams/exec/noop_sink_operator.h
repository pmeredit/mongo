#pragma once

#include "streams/exec/sink_operator.h"

namespace streams {

/**
 * NoOpSinkOperator is used in "process" flows to sample from a stream with no sink.
 */
class NoOpSinkOperator : public SinkOperator {
public:
    NoOpSinkOperator() : SinkOperator(1 /* numInputs */) {}

    int64_t getCount() {
        return _countDocuments;
    }

protected:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) {
        _countDocuments += dataMsg.docs.size();
    }

    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {}
    std::string doGetName() const override {
        return "NoOpSinkOperator";
    }

private:
    int64_t _countDocuments{0};
};

}  // namespace streams
