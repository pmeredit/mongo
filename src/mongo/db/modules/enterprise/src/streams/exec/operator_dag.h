#pragma once

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
    using OperatorContainer = std::vector<std::unique_ptr<Operator>>;

    OperatorDag(OperatorContainer operators, mongo::Pipeline::SourceContainer documentSources)
        : _operators(std::move(operators)), _pipeline(std::move(documentSources)) {}

    /**
     * Start the flow of data through the OperatorDag.
     */
    void start();

    const OperatorContainer& operators() {
        return _operators;
    }

private:
    OperatorContainer _operators;
    mongo::Pipeline::SourceContainer _pipeline;
};

};  // namespace streams
