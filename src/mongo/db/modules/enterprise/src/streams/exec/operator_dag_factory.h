#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/operator_factory.h"

namespace streams {

/**
 * OperatorDagFactory is the main entrypoint for the "frontend" of streams.
 * It takes user-provided pipeline BSON and converts it into an OperatorDag.
 */
class OperatorDagFactory {

public:
    OperatorDagFactory() {}

    std::unique_ptr<OperatorDag> fromBson(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const std::vector<mongo::BSONObj>& userPipeline);

private:
    void validate(const std::vector<mongo::BSONObj>& userPipeline);
    std::unique_ptr<OperatorDag> fromPipeline(
        std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> pipeline);

    OperatorFactory _operatorFactory;
};

};  // namespace streams
