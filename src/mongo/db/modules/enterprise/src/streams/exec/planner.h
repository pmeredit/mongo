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
#include "streams/exec/window_assigner.h"

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
        // TODO: Set this option somehow in StreamManager.
        // Whether to unnest the inner pipeline of a window stage.
        bool unnestWindowPipeline{false};
        // The minimum OperatorId to use for the created Operator instances.
        OperatorId minOperatorId{0};
    };

    Planner(Context* context, Options options);

    /**
     * Creates an OperatorDag from a user supplied BSON array.
     */
    std::unique_ptr<OperatorDag> plan(const std::vector<mongo::BSONObj>& bsonPipeline);

private:
    // Encapsulates state for a $window stage being planned.
    struct WindowPlanningInfo {
        mongo::DocumentSource* stubDocumentSource{nullptr};
        WindowAssigner::Options windowingOptions;
        // Tracks the number of window-aware stages in the inner pipeline of the window stage.
        size_t numWindowAwareStages{0};
        // Tracks the number of window-aware stages that have been planned so far.
        size_t numWindowAwareStagesPlanned{0};
    };

    // Encapsulates state for $lookup stages in a pipeline.
    struct LookUpPlanningInfo {
        // Tracks the count of $lookup stages planned so far.
        size_t numLookupStagesPlanned{0};
        // Tracks the $lookup stages that were rewritten.
        std::vector<std::pair<mongo::BSONObj, mongo::BSONObj>> rewrittenLookupStages;
    };

    // TODO(SERVER-83581): Remove this when old window code is removed.
    bool planningMainPipeline() const {
        return _options.planMainPipeline && !_windowPlanningInfo;
    }

    // Adds the given Operator to '_operators'.
    void appendOperator(std::unique_ptr<Operator> oper);

    // Methods that are used to plan the source stage.
    void planInMemorySource(const mongo::BSONObj& sourceSpec,
                            bool useWatermarks,
                            bool sendIdleMessaes);
    void planSampleSolarSource(const mongo::BSONObj& sourceSpec,
                               bool useWatermarks,
                               bool sendIdleMessaes);
    void planDocumentsSource(const mongo::BSONObj& sourceSpec,
                             bool useWatermarks,
                             bool sendIdleMessages);
    void planKafkaSource(const mongo::BSONObj& sourceSpec,
                         const mongo::KafkaConnectionOptions& baseOptions,
                         bool useWatermarks,
                         bool sendIdleMessaes);
    void planChangeStreamSource(const mongo::BSONObj& sourceSpec,
                                const mongo::AtlasConnectionOptions& atlasOptions,
                                bool useWatermarks,
                                bool sendIdleMessaes);
    void planSource(const mongo::BSONObj& spec, bool useWatermarks, bool sendIdleMessages);

    // Methods that are used to plan the sink stage.
    void planMergeSink(const mongo::BSONObj& spec);
    void planEmitSink(const mongo::BSONObj& spec);

    // Methods that are used to plan a window stage.
    void planTumblingWindow(mongo::DocumentSource* source);
    void planHoppingWindow(mongo::DocumentSource* source);

    // Methods that are used to plan a window stage.
    void planTumblingWindowLegacy(mongo::DocumentSource* source);
    void planHoppingWindowLegacy(mongo::DocumentSource* source);

    // Plans a $group stage.
    void planGroup(mongo::DocumentSource* source);

    // Plans a $sort stage.
    void planSort(mongo::DocumentSource* source);

    // Plans a $limit stage.
    void planLimit(mongo::DocumentSource* source);

    // Plans a $lookup stage.
    void planLookUp(mongo::DocumentSourceLookUp* documentSource);

    // Helper function to prepare the pipeline before sending it to planning. This step includes
    // rewriting the pipeline, parsing the pipeline, optimizing the pipeline and analyzing the
    // pipeline dependency.
    std::pair<std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter>,
              std::unique_ptr<PipelineRewriter>>
    preparePipeline(std::vector<mongo::BSONObj> stages);

    // Plans the stages in the given Pipeline instance.
    void planPipeline(mongo::Pipeline& pipeline,
                      std::unique_ptr<PipelineRewriter> pipelineRewriter);

    // Helper function of plan() that does all the work.
    void planInner(const std::vector<mongo::BSONObj>& bsonPipeline);

    Context* _context{nullptr};
    Options _options;
    // Tracks the state for a $window stage being planned.
    boost::optional<WindowPlanningInfo> _windowPlanningInfo;
    // Tracks state for $lookup stages in a pipeline. The vector has one entry for the top level
    // pipeline and one entry for the inner pipeline of any window stage being planned.
    std::vector<LookUpPlanningInfo> _lookupPlanningInfos;
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
