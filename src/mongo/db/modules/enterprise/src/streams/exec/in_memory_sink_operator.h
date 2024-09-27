/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <deque>

#include "mongo/stdx/mutex.h"
#include "streams/exec/collect_operator.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * This test-only class can act as a sink in an operator dag.
 * You can use it to receive the result documents at the end of the operator dag.
 * This class is thread-safe.
 */
class InMemorySinkOperator : public CollectOperator {
public:
    InMemorySinkOperator(Context* context, int32_t numInputs);

private:
    friend class OutputSamplerTest;

    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;
    void doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    std::string doGetName() const override {
        return "InMemorySinkOperator";
    }

    std::deque<StreamMsgUnion> doGetMessages() override;

    OperatorStats doGetStats() override;

    // Guards _messages.
    mutable mongo::stdx::mutex _mutex;
};

}  // namespace streams
