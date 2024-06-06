/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceUnwind;
class UnwindProcessor;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The operator for $unwind.
 */
class UnwindOperator : public Operator {
public:
    struct Options {
        // DocumentSourceUnwind stage that this Operator wraps.
        mongo::DocumentSourceUnwind* documentSource;
    };

    UnwindOperator(Context* context, Options options);

    mongo::DocumentSourceUnwind* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "UnwindOperator";
    }
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    Options _options;
    mongo::UnwindProcessor* _processor{nullptr};
};

}  // namespace streams
