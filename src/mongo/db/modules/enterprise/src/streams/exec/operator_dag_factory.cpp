/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator_dag_factory.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"

namespace streams {

using namespace std;
using namespace mongo;

unique_ptr<OperatorDag> OperatorDagFactory::fromBson(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const std::vector<BSONObj>& userPipeline) {
    validate(userPipeline);
    return fromPipeline(Pipeline::parse(userPipeline, expCtx));
}

void OperatorDagFactory::validate(const std::vector<BSONObj>& userPipeline) {
    // Prior to this method, some lightweight validation has occured in
    // deserializer: ::mongo::parsePipelineFromBSON, see start_stream_processor.idl.

    // Validate, by name, all the specified stages are supported in streaming
    for (const auto& stage : userPipeline) {
        _operatorFactory.validateByName(stage.firstElementFieldName());
    }
}

unique_ptr<OperatorDag> OperatorDagFactory::fromPipeline(
    unique_ptr<Pipeline, PipelineDeleter> pipeline) {
    pipeline->optimizePipeline();

    vector<unique_ptr<Operator>> operators;
    for (const auto& stage : pipeline->getSources()) {
        operators.emplace_back(_operatorFactory.toOperator(stage.get()));
    }

    // Setup output relationships between operators.
    for (size_t i = 0; i < operators.size(); i++) {
        auto& op = operators[i];
        if (i + 1 < operators.size()) {
            auto& successor = operators[i + 1];
            op->addOutput(successor.get(), 0);
        }
    }

    return std::make_unique<OperatorDag>(std::move(operators), std::move(pipeline->getSources()));
}

};  // namespace streams
