/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <queue>

#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/stages_gen.h"

namespace mongo {
class ExpressionContext;
}

namespace streams {

struct Context;

/**
 * Implements the functionality of $validate stage.
 */
class ValidateOperator : public Operator {
public:
    struct Options {
        std::unique_ptr<mongo::MatchExpression> validator;
        mongo::StreamsValidationActionEnum validationAction;
    };

    ValidateOperator(Context* context, Options options);

private:
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    std::string doGetName() const override {
        return "ValidateOperator";
    }

private:
    Options _options;
};

}  // namespace streams
