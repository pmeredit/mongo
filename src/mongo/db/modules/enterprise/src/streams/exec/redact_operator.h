/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceRedact;
class RedactProcessor;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The operator for $redact.
 */
class RedactOperator : public Operator {
public:
    struct Options {
        // DocumentSourceRedact stage that this Operator wraps.
        mongo::DocumentSourceRedact* documentSource;
    };

    RedactOperator(Context* context, Options options);

    mongo::DocumentSourceRedact* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "RedactOperator";
    }
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    Options _options;
    mongo::RedactProcessor* _processor{nullptr};
};


}  // namespace streams
