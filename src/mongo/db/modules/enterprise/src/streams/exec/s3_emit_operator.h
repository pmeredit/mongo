/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <string>

#include "streams/exec/queued_sink_operator.h"

namespace streams {

class S3EmitOperator : public QueuedSinkOperator {
public:
    struct Options {};

    S3EmitOperator(Context* context, Options opts);

protected:
    std::string doGetName() const override {
        return "S3EmitOperator";
    }

    OperatorStats processDataMsg(StreamDataMsg dataMsg) override;
    void validateConnection() override;

private:
    Options _options;
};

}  // namespace streams
