/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/parser.h"
#include "mongo/db/pipeline/document_source_project.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"

namespace streams {

using namespace std;
using namespace mongo;

namespace {
const std::string kSourceName{"$source"};
const std::string kEmitName{"$emit"};
const std::string kMergeName{"$merge"};
}  // namespace

unique_ptr<OperatorDag> Parser::fromBson(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                         const std::vector<BSONObj>& userPipeline) {
    validate(userPipeline);
    return fromPipeline(Pipeline::parse(userPipeline, expCtx));
}

void Parser::validate(const std::vector<BSONObj>& userPipeline) {
    // Prior to this method, some lightweight validation has occured in
    // deserializer: ::mongo::parsePipelineFromBSON, see start_stream_processor.idl.

    if (_options.enforceSizeGreaterThanZero) {
        if (userPipeline.size() == 0) {
            uasserted(ErrorCode::kTemporaryUserErrorCode, "Pipeline must have at least one stage");
        }
    }

    if (_options.enforceSourceStage) {
        auto firstStageName = userPipeline.front().firstElement().fieldNameStringData();
        if (firstStageName != kSourceName) {
            uasserted(ErrorCode::kTemporaryUserErrorCode,
                      str::stream() << "First stage must be $source, found: " << firstStageName);
        }
    }

    if (_options.enforceOutputStage) {
        auto firstStageName = userPipeline.front().firstElement().fieldNameStringData();
        if (firstStageName != kEmitName && firstStageName != kMergeName) {
            uasserted(ErrorCode::kTemporaryUserErrorCode,
                      str::stream()
                          << "Last stage must be $emit or $merge, found: " << firstStageName);
        }
    }

    // Validate, by name, all the specified stages are supported in streaming
    for (const auto& stage : userPipeline) {
        _operatorFactory.validateByName(stage.firstElementFieldName());
    }
}

unique_ptr<OperatorDag> Parser::fromPipeline(unique_ptr<Pipeline, PipelineDeleter> pipeline) {
    pipeline->optimizePipeline();

    OperatorDag::OperatorContainer operators;
    for (const auto& stage : pipeline->getSources()) {
        auto op = _operatorFactory.toOperator(stage.get());
        if (!operators.empty()) {
            operators.back()->addOutput(op.get(), 0);
        }
        operators.emplace_back(std::move(op));
    }

    return std::make_unique<OperatorDag>(std::move(operators), std::move(pipeline->getSources()));
}

};  // namespace streams
