/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/sink_operator.h"

namespace streams {

/**
 * LogSinkOperator will log all the data and control messages it receives.
 * It is used for testing purposes.
 */
class LogSinkOperator : public SinkOperator {
public:
    LogSinkOperator(Context* context) : SinkOperator(context, 1 /* numInputs */) {}

protected:
    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;
    void doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;
    std::string doGetName() const override {
        return "LogSinkOperator";
    }

private:
    void logControl(StreamControlMsg controlMsg);
};

}  // namespace streams
