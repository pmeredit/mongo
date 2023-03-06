#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/operator_factory.h"

namespace streams {

/**
 * Parser is the main entrypoint for the "frontend" of streams.
 * It takes user-provided pipeline BSON and converts it into an OperatorDag.
 * It's a small wrapper around the existing Pipeline parse and optimize mechanics,
 * plus an OperatorFactory to convert DocumentSource instances to streaming Operators.
 */
class Parser {
public:
    struct Options {
        Options() {}
        Options(bool enforceSizeGreaterThanZero, bool enforceSourceStage, bool enforceOutputStage)
            : enforceSizeGreaterThanZero(enforceSizeGreaterThanZero),
              enforceSourceStage(enforceSourceStage),
              enforceOutputStage(enforceOutputStage) {}

        bool enforceSizeGreaterThanZero{false};
        bool enforceSourceStage{false};
        bool enforceOutputStage{false};
    };

    Parser(Options options = {}) : _options(std::move(options)) {}

    std::unique_ptr<OperatorDag> fromBson(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const std::vector<mongo::BSONObj>& userPipeline);

private:
    void validate(const std::vector<mongo::BSONObj>& userPipeline);
    std::unique_ptr<OperatorDag> fromPipeline(
        std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> pipeline);

    OperatorFactory _operatorFactory;
    Options _options;
};

};  // namespace streams
