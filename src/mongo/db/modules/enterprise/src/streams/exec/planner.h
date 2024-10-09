/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <memory>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/util/string_map.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/pipeline_rewriter.h"
#include "streams/exec/session_window_assigner.h"
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
        // The minimum OperatorId to use for the created Operator instances.
        OperatorId minOperatorId{0};
        // If true, the pipeline is optimized during planning.
        // Set to false when restoring from an execution plan.
        bool shouldOptimize{true};
        // Set to true when validating an modify request when the user specifies
        // resumeFromCheckpoint=true.
        bool shouldValidateModifyRequest{false};
    };

    Planner(Context* context, Options options);

    /**
     * Creates an OperatorDag from a user supplied BSON array.
     */
    std::unique_ptr<OperatorDag> plan(const std::vector<mongo::BSONObj>& bsonPipeline);

    /**
     * Get the list of connection names from the user supplied pipeline.
     */
    static std::vector<mongo::ParsedConnectionInfo> parseConnectionInfo(
        const std::vector<mongo::BSONObj>& pipeline);

private:
    friend class PlannerTest;

    // Encapsulates state for a $window stage being planned.
    struct WindowPlanningInfo {
        mongo::DocumentSource* stubDocumentSource{nullptr};
        std::unique_ptr<WindowAssigner> windowAssigner;
        // Tracks the number of window-aware stages in the inner pipeline of the window stage.
        size_t numWindowAwareStages{0};
        // Tracks the number of blocking window-aware stages in the inner pipeline of the window
        // stage.
        size_t numBlockingWindowAwareStages{0};
        // Tracks the number of window-aware stages that have been planned so far.
        size_t numWindowAwareStagesPlanned{0};
        // Set to true if window's inner pipeline contains a stream_meta dependency before or at the
        // first blocking operator.
        bool streamMetaDependencyBeforeOrAtFirstBlocking{false};
        // Set to true if window's inner pipeline contains a limit operator before the first
        // blocking operator.
        bool limitBeforeFirstBlocking = false;
        // Set to true if session window.
        bool isSessionWindow = false;
    };

    // Encapsulates state for $lookup stages in a pipeline.
    struct LookUpPlanningInfo {
        // Tracks the count of $lookup stages planned so far.
        size_t numLookupStagesPlanned{0};
        // Tracks the $lookup stages that were rewritten.
        std::vector<std::pair<mongo::BSONObj, mongo::BSONObj>> rewrittenLookupStages;
    };

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
    // All return a BSONObj that represents the optimized window stage.
    mongo::BSONObj planTumblingWindow(mongo::DocumentSource* source);
    mongo::BSONObj planHoppingWindow(mongo::DocumentSource* source);
    mongo::BSONObj planSessionWindow(mongo::DocumentSource* source);

    // Utility methods that are used to create dummy operators for session windows.
    void prependDummyLimitOperator(mongo::Pipeline* pipeline);

    // Helper method to create a serialized representation of a window stage with an optimized inner
    // pipeline.
    mongo::BSONObj serializedWindowStage(const std::string& stageName,
                                         mongo::BSONObj spec,
                                         std::vector<mongo::BSONObj> innerPipelineExecutionPlan);

    // Plans a $group stage.
    void planGroup(mongo::DocumentSource* source);

    // Plans a $sort stage.
    void planSort(mongo::DocumentSource* source);

    // Plans a $limit stage.
    void planLimit(mongo::DocumentSource* source);

    // Plans a $lookup stage. Returns a BSONObj that represents the optimized lookup stage.
    mongo::BSONObj planLookUp(mongo::DocumentSourceLookUp* documentSource,
                              mongo::BSONObj serializedPlan);

    // Helper function to prepare the pipeline before sending it to planning. This step includes
    // rewriting the pipeline, parsing the pipeline, optimizing the pipeline and analyzing the
    // pipeline dependency.
    std::pair<std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter>,
              std::unique_ptr<PipelineRewriter>>
    preparePipeline(std::vector<mongo::BSONObj> stages);

    // Plans the stages in the given Pipeline instance.
    std::vector<mongo::BSONObj> planPipeline(mongo::Pipeline& pipeline,
                                             std::unique_ptr<PipelineRewriter> pipelineRewriter);

    // Helper function of plan() that does all the work.
    std::unique_ptr<OperatorDag> planInner(const std::vector<mongo::BSONObj>& bsonPipeline);

    // Helper function that throws an error if hasWindow has already been set, and sets it if not.
    void verifyOneWindowStage();

    // Used to validate a pipeline modify with resumeFromCheckpoint=true. Throws an exception if
    // the modify is not valid.
    void validatePipelineModify(const std::vector<mongo::BSONObj>& oldUserPipeline,
                                const std::vector<mongo::BSONObj>& newUserPipeline);

    Context* _context{nullptr};
    Options _options;
    // True if a window has been planned.
    bool _hasWindow{false};
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
