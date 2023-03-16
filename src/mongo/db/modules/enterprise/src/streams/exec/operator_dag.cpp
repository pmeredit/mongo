/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator_dag.h"

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

};  // namespace streams
