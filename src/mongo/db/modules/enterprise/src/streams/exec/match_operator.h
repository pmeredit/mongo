/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceMatch;
class MatchProcessor;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The operator for $match.
 */
class MatchOperator : public Operator {
public:
    struct Options {
        // DocumentSourceMatch stage that this Operator wraps.
        mongo::DocumentSourceMatch* documentSource;
    };

    MatchOperator(Context* context, Options options);

    mongo::DocumentSourceMatch* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "MatchOperator";
    }
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    Options _options;
    mongo::MatchProcessor* _processor{nullptr};
};


}  // namespace streams
