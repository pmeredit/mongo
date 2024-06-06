/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/sink_operator.h"

namespace streams {

/**
 * NoOpSinkOperator is used in "process" flows to sample from a stream with no sink.
 */
class NoOpSinkOperator : public SinkOperator {
public:
    NoOpSinkOperator(Context* context) : SinkOperator(context, 1 /* numInputs */) {}

protected:
    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override {
        _stats.numOutputDocs = _stats.numInputDocs;
        _stats.numOutputBytes = _stats.numInputBytes;
        sendOutputToSamplers(std::move(dataMsg));
    }

    std::string doGetName() const override {
        return "NoOpSinkOperator";
    }
};

}  // namespace streams
