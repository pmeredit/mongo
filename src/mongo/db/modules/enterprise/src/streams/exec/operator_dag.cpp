/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator_dag.h"

namespace streams {

void OperatorDag::start() {
    for (auto i = _operators.rbegin(); i != _operators.rend(); i++) {
        i->get()->start();
    }
}

};  // namespace streams
