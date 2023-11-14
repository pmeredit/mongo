#pragma once

#include <memory>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/pipeline_rewriter.h"
#include "streams/exec/stages_gen.h"

using namespace mongo::literals;

namespace mongo {
class Connection;
class DocumentSourceLookUp;
class ExpressionContext;
}  // namespace mongo

namespace streams {

class SinkOperator;
class SourceOperator;
struct Context;

/**
 * Planner is the main entrypoint for the "frontend" of streams.
 * It takes user-provided pipeline BSON and converts it into an OperatorDag.
 * It's a small wrapper around the existing Pipeline parse and optimize mechanics.
 * A separate instance of Planner should be used per stream processor.
 */
class Planner {
public:
    struct Options {
        // If true, caller is planning the main/outer pipeline.
        // If false, caller is planning the inner pipeline of a window stage.
        bool planMainPipeline{true};
        // The minimum OperatorId to use for the created Operator instances.
        OperatorId minOperatorId{0};
    };

    Planner(Context* context, Options options);

    /**
     * Creates an OperatorDag from a user supplied BSON array.
     */
    std::unique_ptr<OperatorDag> plan(const std::vector<mongo::BSONObj>& bsonPipeline);

private:
    // Verifies that a stage specified in the input pipeline is a valid stage.
    void validateByName(const std::string& name);

    // Adds the given Operator to '_operators'.
    void appendOperator(std::unique_ptr<Operator> oper);

    // Methods that are used to plan the source stage.
    void planInMemorySource(const mongo::BSONObj& sourceSpec, bool useWatermarks);
    void planSampleSolarSource(const mongo::BSONObj& sourceSpec, bool useWatermarks);
    void planKafkaSource(const mongo::BSONObj& sourceSpec,
                         const mongo::KafkaConnectionOptions& baseOptions,
                         bool useWatermarks);
    void planChangeStreamSource(const mongo::BSONObj& sourceSpec,
                                const mongo::AtlasConnectionOptions& atlasOptions,
                                bool useWatermarks);
    void planSource(const mongo::BSONObj& spec, bool useWatermarks);

    // Methods that are used to plan the sink stage.
    void planMergeSink(const mongo::BSONObj& spec);
    void planEmitSink(const mongo::BSONObj& spec);

    // Methods that are used to plan a window stage.
    void planTumblingWindow(mongo::DocumentSource* source);
    void planHoppingWindow(mongo::DocumentSource* source);

    // Plans a lookup stage.
    void planLookUp(const mongo::BSONObj& stageObj, mongo::DocumentSourceLookUp* documentSource);

    // Plans the stages in the given Pipeline instance.
    void planPipeline(const mongo::Pipeline& pipeline);

    Context* _context{nullptr};
    Options _options;
    std::unique_ptr<PipelineRewriter> _pipelineRewriter;
    std::unique_ptr<DocumentTimestampExtractor> _timestampExtractor;
    std::unique_ptr<EventDeserializer> _eventDeserializer;
    // Tracks all the DocumentSource instances used in the plan.
    mongo::Pipeline::SourceContainer _pipeline;
    // Tracks all the Operator instances added to the plan so far.
    OperatorDag::OperatorContainer _operators;
    // Tracks the next OperatorId to assign to an Operator in the plan.
    OperatorId _nextOperatorId{0};
};

};  // namespace streams
