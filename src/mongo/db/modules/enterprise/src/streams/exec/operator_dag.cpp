/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/operator_dag.h"

#include "streams/exec/sink_operator.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/window_aware_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

void OperatorDag::cleanupOperators(size_t index) {
    tassert(ErrorCodes::InternalError, "_operators is empty", !_operators.empty());
    // Either all operators should be started or none
    for (size_t j = _operators.size() - 1; j >= index; j--) {
        try {
            _operators[j]->stop();
        } catch (const DBException& e) {
            // swallow stop error if it occurs.
            LOGV2_WARNING(10206701,
                          "Error occurred while stopping operator after start error",
                          "context"_attr = _options.loggingContext,
                          "operator"_attr = _operators[j]->getName(),
                          "error"_attr = e.what());
        } catch (const SPException& e) {
            // swallow stop error if it occurs.
            LOGV2_WARNING(10206702,
                          "Error occurred while stopping operator after start error",
                          "context"_attr = _options.loggingContext,
                          "operator"_attr = _operators[j]->getName(),
                          "error"_attr = e.what());
        }
    }
}

void OperatorDag::start() {
    // Start the operators in sink to source order.
    int i = _operators.size() - 1;
    try {
        for (; i >= 0; i--) {
            _operators[i]->start();
        }
    } catch (const DBException&) {
        cleanupOperators(i);
        throw;
    } catch (const SPException&) {
        cleanupOperators(i);
        throw;
    }
}

void OperatorDag::stop() {
    // Stop the operators in source to sink order.
    for (auto& i : _operators) {
        i->stop();
    }
    _operators.clear();
}

SourceOperator* OperatorDag::source() const {
    tassert(ErrorCodes::InternalError, "_operators is empty", !_operators.empty());
    return dynamic_cast<SourceOperator*>(_operators.front().get());
}

SinkOperator* OperatorDag::sink() const {
    tassert(ErrorCodes::InternalError, "_operators is empty", !_operators.empty());
    return dynamic_cast<SinkOperator*>(_operators.back().get());
}

bool OperatorDag::shouldReportLatency() {
    for (const auto& i : _operators) {
        if (dynamic_cast<WindowAwareOperator*>(i.get())) {
            return false;
        }
    }
    return true;
}

};  // namespace streams
