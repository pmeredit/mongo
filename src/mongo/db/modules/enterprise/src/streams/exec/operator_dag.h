#pragma once

#include <deque>

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "streams/exec/operator.h"

namespace streams {

/**
 * OperatorDag is an directed, acyclic graph of stream Operators.
 * Use OperatorDagFactory to create an OperatorDag.
 */
class OperatorDag {
public:
    using OperatorContainer = std::deque<std::unique_ptr<Operator>>;

    OperatorDag(OperatorContainer operators, mongo::Pipeline::SourceContainer documentSources)
        : _operators(std::move(operators)), _pipeline(std::move(documentSources)) {}

    // Start the flow of data through the OperatorDag.
    // TODO: This can throw an exception (e.g. KafkaConsumerOperator::start()), handle the
    // exception.
    void start();

    // Stop the flow of data through the OperatorDag.
    void stop();

    // Returns the first operator in _operators.
    Operator* source() {
        return _operators.front().get();
    }

    // Returns the last operator in _operators.
    Operator* sink() {
        return _operators.back().get();
    }

    const OperatorContainer& operators() {
        return _operators;
    }

    // Adds the given operator at the end of _operators.
    void pushBack(std::unique_ptr<Operator> oper) {
        _operators.push_back(std::move(oper));
    }

    // Adds the given operator at the beginning of _operators.
    void pushFront(std::unique_ptr<Operator> oper) {
        _operators.push_front(std::move(oper));
    }

private:
    OperatorContainer _operators;
    mongo::Pipeline::SourceContainer _pipeline;
};

};  // namespace streams
