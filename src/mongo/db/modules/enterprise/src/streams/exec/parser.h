#pragma once

#include <memory>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/operator_factory.h"
#include "streams/exec/pipeline_rewriter.h"
#include "streams/exec/stages_gen.h"

using namespace mongo::literals;

namespace mongo {
class Connection;
}

namespace streams {

struct Context;

/**
 * Parser is the main entrypoint for the "frontend" of streams.
 * It takes user-provided pipeline BSON and converts it into an OperatorDag.
 * It's a small wrapper around the existing Pipeline parse and optimize mechanics,
 * plus an OperatorFactory to convert DocumentSource instances to streaming Operators.
 * A separate instance of Parser should be used per stream processor.
 */
class Parser {
public:
    struct Options {
        // If true, caller is planning the main/outer pipeline.
        // If false, caller is planning the inner pipeline of a window stage.
        bool planMainPipeline{true};
    };

    Parser(Context* context,
           Options options,
           mongo::stdx::unordered_map<std::string, mongo::Connection> connections = {});

    /**
     * Creates an OperatorContainer from a Pipeline and assigns operator IDs.
     */
    OperatorDag::OperatorContainer fromPipeline(const mongo::Pipeline& pipeline,
                                                OperatorId minOperatorId) const;

    /**
     * Creates an OperatorDag from a user supplied BSON array.
     */
    std::unique_ptr<OperatorDag> fromBson(const std::vector<mongo::BSONObj>& bsonPipeline);

private:
    /**
     * Create an OperatorContainer from a Pipeline without assinging operator IDs.
     */
    OperatorDag::OperatorContainer fromPipeline(const mongo::Pipeline& pipeline) const;

    Context* _context{nullptr};
    Options _options;
    std::unique_ptr<PipelineRewriter> _pipelineRewriter;
    std::unique_ptr<OperatorFactory> _operatorFactory;
    mongo::stdx::unordered_map<std::string, mongo::Connection> _connectionObjs;
};

};  // namespace streams
