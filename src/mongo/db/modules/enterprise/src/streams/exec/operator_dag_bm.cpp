/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <benchmark/benchmark.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <chrono>
#include <exception>
#include <fmt/format.h>
#include <functional>
#include <string>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/exec/sbe/abt/abt_lower.h"
#include "mongo/db/exec/sbe/abt/sbe_abt_test_util.h"
#include "mongo/db/exec/sbe/expressions/runtime_environment.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/pipeline/abt/document_source_visitor.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/db/query/optimizer/explain.h"
#include "mongo/db/query/optimizer/opt_phase_manager.h"
#include "mongo/db/query/optimizer/utils/unit_test_utils.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/plan_executor_factory.h"
#include "mongo/platform/random.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/context.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/executor.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/message.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace mongo {
namespace {

using namespace optimizer;
using namespace streams;

class OperatorDagBMFixture : public benchmark::Fixture {
public:
    OperatorDagBMFixture();

    void SetUp(benchmark::State& state) override;
    void TearDown(benchmark::State& state) override;

    void runStreamProcessor(benchmark::State& state, const BSONObj& pipelineSpec);
    void runAggregationPipeline(benchmark::State& state, const BSONObj& pipelineSpec);
    void runSBEAggregationPipeline(benchmark::State& state, const BSONObj& pipelineSpec);

    void runDeserializerBenchmark(JsonEventDeserializer deserializer, benchmark::State& state);

protected:
    std::string generateRandomString(size_t size);

    BSONObj generateBSONObj(int numFields);

    // Generates a document. Each document has 200 fields in it and is ~13KB in size.
    BSONObj generateDoc();

    static constexpr int32_t kNumDataMsgs{100};
    static constexpr int32_t kDocsPerMsg{1000};

    PseudoRandom _random;
    std::unique_ptr<MetricManager> _metricManager;

    std::vector<BSONObj> _addFieldsObjs;
    std::vector<BSONObj> _matchObjs;
    BSONObj _tumblingWindowObj;
    BSONObj _groupObj;
    StreamDataMsg _dataMsg;
    std::vector<BSONObj> _inputObjs;
};

OperatorDagBMFixture::OperatorDagBMFixture()
    : _random(/*seed*/ 1), _metricManager(std::make_unique<MetricManager>()) {
    _addFieldsObjs.resize(6);
    _matchObjs.resize(3);
    _addFieldsObjs[0] = fromjson(R"(
        { $addFields: {
            multField1: { $multiply: [ "$intField0", "$intField1" ] }
        }})");
    _addFieldsObjs[1] = fromjson(R"(
        { $addFields: {
            multField2: { $multiply: [ "$intField2", "$intField3" ] }
        }})");
    _addFieldsObjs[2] = fromjson(R"(
        { $addFields: {
            multField3: { $multiply: [ "$intField4", "$intField5" ] }
        }})");
    _addFieldsObjs[3] = fromjson(R"(
        { $addFields: {
            multField4: { $multiply: [ "$subObj.intField0", "$subObj.intField1" ] }
        }})");
    _addFieldsObjs[4] = fromjson(R"(
        { $addFields: {
            multField5: { $multiply: [ "$subObj.intField2", "$subObj.intField3" ] }
        }})");
    _addFieldsObjs[5] = fromjson(R"(
        { $addFields: {
            multField6: { $multiply: [ "$subObj.intField4", "$subObj.intField5" ] }
        }})");
    _matchObjs[0] = fromjson(R"(
        { $match: {
            $and: [
                { intField6: { $lt: 5000 } },
                { intField7: { $lt: 5000 } },
                { "subObj.intField6": { $lt: 5000 } },
                { "subObj.intField7": { $lt: 5000 } }
            ]
        }})");
    _matchObjs[1] = fromjson(R"(
        { $match: {
            $and: [
                { intField8: { $gte: 0 } },
                { intField9: { $gte: 0 } },
                { "subObj.intField8": { $gte: 0 } },
                { "subObj.intField9": { $gte: 0 } }
            ]
        }})");
    _matchObjs[2] = fromjson(R"(
        { $match: {
            $and: [
                { $expr: { $isNumber: "$intField6" } },
                { $expr: { $isNumber: "$intField7" } },
                { $expr: { $isNumber: "$subObj.intField6" } },
                { $expr: { $isNumber: "$subObj.intField7" } }
            ]
        }})");
    _tumblingWindowObj = fromjson(R"(
        { $tumblingWindow: {
            interval: {size: NumberInt(100), unit: "ms"},
            allowedLateness: {size: NumberInt(0), unit: "second"},
            pipeline: [
                {
                    $group: {
                        _id: null,
                        TotalNumRecords: {$sum: 1},
                        TotalIntField0: {$sum: "$intField0"},
                        TotalIntField1: {$sum: "$intField1"},
                        TotalIntField2: {$sum: "$intField2"},
                        TotalIntField3: {$sum: "$intField3"},
                        TotalIntField4: {$sum: "$intField4"},
                        TotalIntField5: {$sum: "$intField5"},
                        TotalIntField6: {$sum: "$intField6"},
                        TotalIntField7: {$sum: "$intField7"},
                        TotalIntField8: {$sum: "$intField8"},
                        TotalIntField9: {$sum: "$intField9"},
                        TotalSubObjIntField0: {$sum: "$subObj.intField0"},
                        TotalSubObjIntField1: {$sum: "$subObj.intField1"},
                        TotalSubObjIntField2: {$sum: "$subObj.intField2"},
                        TotalSubObjIntField3: {$sum: "$subObj.intField3"},
                        TotalSubObjIntField4: {$sum: "$subObj.intField4"},
                        TotalSubObjIntField5: {$sum: "$subObj.intField5"},
                        TotalSubObjIntField6: {$sum: "$subObj.intField6"},
                        TotalSubObjIntField7: {$sum: "$subObj.intField7"},
                        TotalSubObjIntField8: {$sum: "$subObj.intField8"},
                        TotalSubObjIntField9: {$sum: "$subObj.intField9"}
                    }
                }
            ]
        }})");
    _groupObj = fromjson(R"(
        { $group: {
            _id: null,
            TotalNumRecords: {$sum: 1},
            TotalIntField0: {$sum: "$intField0"},
            TotalIntField1: {$sum: "$intField1"},
            TotalIntField2: {$sum: "$intField2"},
            TotalIntField3: {$sum: "$intField3"},
            TotalIntField4: {$sum: "$intField4"},
            TotalIntField5: {$sum: "$intField5"},
            TotalIntField6: {$sum: "$intField6"},
            TotalIntField7: {$sum: "$intField7"},
            TotalIntField8: {$sum: "$intField8"},
            TotalIntField9: {$sum: "$intField9"},
            TotalSubObjIntField0: {$sum: "$subObj.intField0"},
            TotalSubObjIntField1: {$sum: "$subObj.intField1"},
            TotalSubObjIntField2: {$sum: "$subObj.intField2"},
            TotalSubObjIntField3: {$sum: "$subObj.intField3"},
            TotalSubObjIntField4: {$sum: "$subObj.intField4"},
            TotalSubObjIntField5: {$sum: "$subObj.intField5"},
            TotalSubObjIntField6: {$sum: "$subObj.intField6"},
            TotalSubObjIntField7: {$sum: "$subObj.intField7"},
            TotalSubObjIntField8: {$sum: "$subObj.intField8"},
            TotalSubObjIntField9: {$sum: "$subObj.intField9"}
        }})");
}

void OperatorDagBMFixture::SetUp(benchmark::State& state) {
    if (state.thread_index == 0) {
        auto service = ServiceContext::make();
        setGlobalServiceContext(std::move(service));
    }

    invariant(kDocsPerMsg % 10 == 0);

    _dataMsg = StreamDataMsg{};
    _dataMsg.docs.reserve(kDocsPerMsg);
    _inputObjs.reserve(kDocsPerMsg);
    // Generate 10 unique docs and duplicate them as many times as needed.
    for (int i = 0; i < 10; i++) {
        auto obj = generateDoc();
        _dataMsg.docs.emplace_back(Document(obj));
        _inputObjs.emplace_back(std::move(obj));
    }
    for (int i = 10; i < kDocsPerMsg; i += 10) {
        std::copy_n(_dataMsg.docs.begin(), 10, std::back_inserter(_dataMsg.docs));
        std::copy_n(_inputObjs.begin(), 10, std::back_inserter(_inputObjs));
    }
    invariant(_inputObjs.size() == kDocsPerMsg);
    invariant(_dataMsg.docs.size() == kDocsPerMsg);
}

void OperatorDagBMFixture::TearDown(benchmark::State& state) {
    _dataMsg = StreamDataMsg{};
    _inputObjs.clear();
    if (state.thread_index == 0) {
        setGlobalServiceContext({});
    }
}

std::string OperatorDagBMFixture::generateRandomString(size_t size) {
    static const std::string kAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string str;
    str.reserve(size);
    for (size_t j = 0; j < size; ++j) {
        str.push_back(kAlphabet[_random.nextInt32(kAlphabet.size())]);
    }
    return str;
}

BSONObj OperatorDagBMFixture::generateBSONObj(int numFields) {
    BSONObjBuilder objBuilder;

    int numIntFields = numFields / 2;
    for (int i = 0; i < numIntFields; ++i) {
        objBuilder.append(fmt::format("intField{}", i), _random.nextInt64(/*max*/ 1000));
    }
    int numStrFields = numFields / 2;
    for (int i = 0; i < numStrFields; ++i) {
        objBuilder.append(fmt::format("strField{}", i), generateRandomString(/*size*/ 100));
    }
    return objBuilder.obj();
}

BSONObj OperatorDagBMFixture::generateDoc() {
    BSONObjBuilder objBuilder(generateBSONObj(20));
    objBuilder.append("subObj", generateBSONObj(20));
    return objBuilder.obj();
}

void OperatorDagBMFixture::runStreamProcessor(benchmark::State& state,
                                              const BSONObj& pipelineSpec) {
    QueryTestServiceContext qtServiceContext;
    auto svcCtx = qtServiceContext.getServiceContext();

    auto [context, _] = getTestContext(svcCtx);
    context->connections = testInMemoryConnectionRegistry();

    auto bsonPipelineVector = parsePipelineFromBSON(pipelineSpec["pipeline"]);

    for (auto keepRunning : state) {
        // Create a streaming DAG from the user JSON
        Planner planner(context.get(), {.unnestWindowPipeline = true});
        std::unique_ptr<OperatorDag> dag(planner.plan(bsonPipelineVector));
        invariant(dag->operators().size() == 10);
        std::cout << "Operators in the pipeline: ";
        for (auto& oper : dag->operators()) {
            std::cout << oper->getName() << " -> ";
        }
        std::cout << std::endl;

        // Add an in-memory source.
        auto sourcePtr = dynamic_cast<InMemorySourceOperator*>(dag->source());
        invariant(sourcePtr);
        auto sinkPtr = dynamic_cast<InMemorySinkOperator*>(dag->sink());
        invariant(sinkPtr);

        // Register metrics for all operators.
        for (const auto& oper : dag->operators()) {
            oper->registerMetrics(_metricManager.get());
        }

        // Start the dag.
        dag->start();

        StreamControlMsg controlMsg;
        controlMsg.watermarkMsg = WatermarkControlMsg{};
        for (int i = 0; i < kNumDataMsgs; i++) {
            // Generate watermarks such that each StreamDataMsg is in its own window.
            for (auto& streamDoc : _dataMsg.docs) {
                streamDoc.minEventTimestampMs = i * 1000;
                streamDoc.maxEventTimestampMs = i * 1000;
            }
            controlMsg.watermarkMsg = WatermarkControlMsg{.eventTimeWatermarkMs = i * 1000};
            sourcePtr->addDataMsg(_dataMsg, controlMsg);
            sourcePtr->runOnce();
        }
        controlMsg.watermarkMsg->eventTimeWatermarkMs += 1000;
        sourcePtr->addControlMsg(std::move(controlMsg));
        sourcePtr->runOnce();

        // Verify the correct output.
        auto opMessages = sinkPtr->getMessages();
        size_t totalMessages = 0;
        while (!opMessages.empty()) {
            const auto& msg = opMessages.front();
            if (msg.dataMsg) {
                totalMessages += msg.dataMsg->docs.size();
            }
            opMessages.pop_front();
        }
        ASSERT_EQ(totalMessages, kNumDataMsgs);

        dag->stop();
    }
}

void OperatorDagBMFixture::runAggregationPipeline(benchmark::State& state,
                                                  const BSONObj& pipelineSpec) {
    QueryTestServiceContext qtServiceContext;
    auto svcCtx = qtServiceContext.getServiceContext();
    auto [context, _] = getTestContext(svcCtx);

    auto bsonPipelineVector = parsePipelineFromBSON(pipelineSpec["pipeline"]);

    for (auto keepRunning : state) {
        auto aggPipeline = Pipeline::parse(bsonPipelineVector, context->expCtx);
        aggPipeline->optimizePipeline();

        auto feeder = boost::intrusive_ptr<DocumentSourceFeeder>(
            new DocumentSourceFeeder(aggPipeline->getContext()));
        aggPipeline->addInitialSource(feeder);

        invariant(aggPipeline->getSources().size() == 9);
        std::cout << "Stages in the pipeline: ";
        for (const auto& stage : aggPipeline->getSources()) {
            std::cout << stage->getSourceName() << " -> ";
        }
        std::cout << std::endl;

        int32_t numDataMsgsSent{0};
        int32_t totalMessages = 0;
        auto result = aggPipeline->getSources().back()->getNext();
        while (true) {
            if (result.isAdvanced()) {
                ++totalMessages;
                ASSERT_EQ(result.getDocument()["TotalNumRecords"].getInt(),
                          kNumDataMsgs * kDocsPerMsg);
            } else if (result.isPaused()) {
                if (numDataMsgsSent < kNumDataMsgs) {
                    for (auto& streamDoc : _dataMsg.docs) {
                        feeder->addDocument(streamDoc.doc);
                    }
                    ++numDataMsgsSent;
                } else {
                    // Send EOF message.
                    feeder->setEndOfBufferSignal(DocumentSource::GetNextResult::makeEOF());
                }
            } else {
                ASSERT(result.isEOF());
                break;
            }
            result = aggPipeline->getSources().back()->getNext();
        }
        ASSERT(result.isEOF());
        ASSERT_EQ(totalMessages, 1);
    }
}

void OperatorDagBMFixture::runSBEAggregationPipeline(benchmark::State& state,
                                                     const BSONObj& pipelineSpec) {
    QueryTestServiceContext qtServiceContext;
    auto svcCtx = qtServiceContext.getServiceContext();
    auto [context, _] = getTestContext(svcCtx);

    // Following code to generate an SBE plan from an aggregation pipeline is copied from
    // runSBEAST() in sbe_abt_test_util.cpp
    // TODO: We are currently using a test utility to create an SBE plan. We should probably look
    // into using classic query optimizer and SBE stage builder directly instead. The only reason
    // we don't do it currently is because $addFields is not supported on this path.

    auto prefixId = PrefixId::createForTests();
    Metadata metadata{{}};

    auto bsonPipelineVector = parsePipelineFromBSON(pipelineSpec["pipeline"]);
    auto pipeline = parsePipeline(bsonPipelineVector,
                                  NamespaceString::createNamespaceString_forTest("test"),
                                  context->opCtx.get());

    ABT valueArray = createValueArray(_inputObjs);

    const ProjectionName scanProjName = prefixId.getNextId("scan");
    QueryParameterMap qp;
    ABT tree = translatePipelineToABT(metadata,
                                      *pipeline.get(),
                                      scanProjName,
                                      make<ValueScanNode>(ProjectionNameVector{scanProjName},
                                                          boost::none,
                                                          std::move(valueArray),
                                                          true /*hasRID*/),
                                      prefixId,
                                      qp);
    // std::cout << "SBE translated ABT: " << ExplainGenerator::explainV2(tree) << std::endl;

    auto phaseManager = makePhaseManager(OptPhaseManager::getAllProdRewrites(),
                                         prefixId,
                                         {{}},
                                         boost::none /*costModel*/,
                                         DebugInfo::kDefaultForTests);

    PlanAndProps planAndProps = phaseManager.optimizeAndReturnProps(std::move(tree));

    SlotVarMap map;
    boost::optional<sbe::value::SlotId> ridSlot;
    auto runtimeEnv = std::make_unique<sbe::RuntimeEnvironment>();
    sbe::value::SlotIdGenerator ids;
    sbe::InputParamToSlotMap inputParamToSlotMap;

    auto env = VariableEnvironment::build(planAndProps._node);
    SBENodeLowering g{
        env, *runtimeEnv, ids, inputParamToSlotMap, phaseManager.getMetadata(), planAndProps._map};
    auto sbePlan = g.optimize(planAndProps._node, map, ridSlot);
    ASSERT_EQ(1, map.size());
    ASSERT(!ridSlot);
    ASSERT(sbePlan != nullptr);

    sbe::CompileCtx ctx(std::move(runtimeEnv));
    sbePlan->prepare(ctx);

    std::vector<sbe::value::SlotAccessor*> accessors;
    for (auto& [name, slot] : map) {
        accessors.emplace_back(sbePlan->getAccessor(ctx, slot));
    }
    // For now assert we only have one final projection.
    ASSERT_EQ(1, accessors.size());

    sbePlan->attachToOperationContext(context->opCtx.get());
    // std::cout << "plan: " << sbe::DebugPrinter().print(*sbePlan) << std::endl;

    for (auto keepRunning : state) {
        for (int i = 0; i < kNumDataMsgs; i++) {
            sbePlan->open(/*reOpen*/ false);
            std::vector<BSONObj> outputObjs;
            while (sbePlan->getNext() != sbe::PlanState::IS_EOF) {
                auto [tag, val] = accessors.at(0)->getViewOfValue();
                ASSERT(sbe::value::isObject(tag));
                if (tag == sbe::value::TypeTags::Object) {
                    BSONObjBuilder bb;
                    sbe::bson::convertToBsonObj(bb, sbe::value::getObjectView(val));
                    outputObjs.push_back(bb.obj());
                } else {
                    // Must be a bsonObj.
                    outputObjs.push_back(BSONObj{sbe::value::bitcastTo<const char*>(val)});
                }
            };
            sbePlan->close();

            ASSERT_EQ(outputObjs.size(), 1);
            ASSERT_EQ(outputObjs[0]["TotalNumRecords"].Int(), kDocsPerMsg);
        }
    }
}

// The only difference between Type1 and Type2 benchmark is that Type1 use isNumber function.
BENCHMARK_F(OperatorDagBMFixture, BM_RunStreamProcessorType1)(benchmark::State& state) {
    const auto pipelineSpec =
        BSON("pipeline" << BSON_ARRAY(getTestSourceSpec()
                                      << _addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                      << _addFieldsObjs[3] << _addFieldsObjs[4] << _addFieldsObjs[5]
                                      << _matchObjs[2] << _matchObjs[1] << _tumblingWindowObj
                                      << getTestMemorySinkSpec()));
    runStreamProcessor(state, pipelineSpec);
}

BENCHMARK_F(OperatorDagBMFixture, BM_RunAggregationPipelineType1)(benchmark::State& state) {
    const auto pipelineSpec =
        BSON("pipeline" << BSON_ARRAY(_addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                                        << _addFieldsObjs[3] << _addFieldsObjs[4]
                                                        << _addFieldsObjs[5] << _matchObjs[2]
                                                        << _matchObjs[1] << _groupObj));
    runAggregationPipeline(state, pipelineSpec);
}

// We cannot run Type1 benchmark for SBE as ABT does not support isNumber function yet.

BENCHMARK_F(OperatorDagBMFixture, BM_RunStreamProcessorType2)(benchmark::State& state) {
    const auto pipelineSpec =
        BSON("pipeline" << BSON_ARRAY(getTestSourceSpec()
                                      << _addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                      << _addFieldsObjs[3] << _addFieldsObjs[4] << _addFieldsObjs[5]
                                      << _matchObjs[0] << _matchObjs[1] << _tumblingWindowObj
                                      << getTestMemorySinkSpec()));
    runStreamProcessor(state, pipelineSpec);
}

BENCHMARK_F(OperatorDagBMFixture, BM_RunAggregationPipelineType2)(benchmark::State& state) {
    const auto pipelineSpec =
        BSON("pipeline" << BSON_ARRAY(_addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                                        << _addFieldsObjs[3] << _addFieldsObjs[4]
                                                        << _addFieldsObjs[5] << _matchObjs[0]
                                                        << _matchObjs[1] << _groupObj));
    runAggregationPipeline(state, pipelineSpec);
}

BENCHMARK_F(OperatorDagBMFixture, BM_RunSBEAggregationPipelineType2)(benchmark::State& state) {
    const auto pipelineSpec =
        BSON("pipeline" << BSON_ARRAY(_addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                                        << _addFieldsObjs[3] << _addFieldsObjs[4]
                                                        << _addFieldsObjs[5] << _matchObjs[0]
                                                        << _matchObjs[1] << _groupObj));
    runSBEAggregationPipeline(state, pipelineSpec);
}

void OperatorDagBMFixture::runDeserializerBenchmark(JsonEventDeserializer deserializer,
                                                    benchmark::State& state) {
    // Setup the input.
    const int inputSize = 100000;
    std::vector<std::string> input;
    input.reserve(inputSize);
    for (int i = 0; i < inputSize; ++i) {
        auto obj = BSON("a" << generateRandomString(10) << "b" << generateRandomString(20) << "c"
                            << generateRandomString(30) << "d" << generateRandomString(40) << "e"
                            << generateRandomString(50));
        input.push_back(tojson(obj));
    }
    // Setup the output buffer.
    std::vector<BSONObj> results(inputSize);

    // Run the benchmark.
    for (auto keepRunning : state) {
        for (int i = 0; i < inputSize; ++i) {
            results[i] = deserializer.deserialize(input[i].c_str(), input[i].size());
        }
    }
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializer)(benchmark::State& state) {
    runDeserializerBenchmark(JsonEventDeserializer(), state);
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializerForceSlowPath)(benchmark::State& state) {
    runDeserializerBenchmark(JsonEventDeserializer(JsonEventDeserializer::Options{
                                 .allowBsonCxxParsing = true, .forceBsonCxxParsing = true}),
                             state);
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializerDisableSlowPath)(benchmark::State& state) {
    runDeserializerBenchmark(
        JsonEventDeserializer(JsonEventDeserializer::Options{.allowBsonCxxParsing = false}), state);
}


}  // namespace
}  // namespace mongo
