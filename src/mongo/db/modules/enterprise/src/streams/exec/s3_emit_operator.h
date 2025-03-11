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

    S3EmitOperator(Context* context, Options options);

    // Make a SinkWriter instance.
    std::unique_ptr<SinkWriter> makeWriter(int id) override;

    std::string doGetName() const override {
        return "S3EmitOperator";
    }

    mongo::ConnectionTypeEnum getConnectionType() const override {
        // TODO(SERVER-100795): Use the right connection type here.
        return mongo::ConnectionTypeEnum::HTTPS;
    }

private:
    Options _options;
};

class S3EmitWriter : public SinkWriter {
public:
    S3EmitWriter(Context* context, SinkOperator* sinkOperator)
        : SinkWriter(context, sinkOperator) {}

protected:
    OperatorStats processDataMsg(StreamDataMsg dataMsg) override;
    void connect() override;
};

}  // namespace streams
