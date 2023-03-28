#pragma once

#include "streams/exec/operator.h"

namespace streams {

/**
 * LogSinkOperator will log all the data and control messages it receives.
 * It is used for testing purposes.
 */
class LogSinkOperator : public Operator {
public:
    LogSinkOperator() : Operator(1 /* numInputs */, 0 /* numOutputs */) {}

protected:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg);
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg);
    std::string doGetName() const override {
        return "LogSinkOperator";
    }

private:
    void logControl(StreamControlMsg controlMsg);
};

}  // namespace streams
