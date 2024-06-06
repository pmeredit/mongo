/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/operator_dag.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/source_operator.h"

namespace streams {

void OperatorDag::start() {
    // Start the operators in sink to source order.
    for (auto i = _operators.rbegin(); i != _operators.rend(); i++) {
        (*i)->start();
    }
}

void OperatorDag::stop() {
    // Stop the operators in source to sink order.
    for (auto& i : _operators) {
        i->stop();
    }
}

SourceOperator* OperatorDag::source() const {
    return dynamic_cast<SourceOperator*>(_operators.front().get());
}

SinkOperator* OperatorDag::sink() const {
    return dynamic_cast<SinkOperator*>(_operators.back().get());
}

};  // namespace streams
