#pragma once

#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace streams {

/**
 * The operator for $limit.
 */
class LimitOperator : public Operator {
public:
    LimitOperator(Context* context, int64_t limit)
        : Operator(context, /*numInputs*/ 1, /*numOutputs*/ 1), _limit(limit) {}

protected:
    std::string doGetName() const override {
        return "LimitOperator";
    }

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    int64_t _limit{0};
    int64_t _numSent{0};
};

}  // namespace streams
