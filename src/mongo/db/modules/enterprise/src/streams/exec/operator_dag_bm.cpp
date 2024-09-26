/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
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
#include "mongo/db/exec/sbe/expressions/runtime_environment.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
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

    void runDeserializerBenchmark(benchmark::State& state,
                                  JsonEventDeserializer deserializer,
                                  const std::vector<std::string>& inputDocs);

protected:
    std::string generateRandomString(size_t size);

    BSONObj generateBSONObj(int numFields, int stringFieldSize);

    BSONObj generateDoc(int stringFieldSize);

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
    std::vector<std::string> _inputDocsSmall;
    std::vector<std::string> _inputDocsMedium;
    std::vector<std::string> _inputDocsLarge;
    std::vector<std::string> _inputDocsXLarge;
    std::vector<std::string> _inputDocsXXLarge;
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
    _inputDocsSmall.reserve(kDocsPerMsg);
    _inputDocsMedium.reserve(kDocsPerMsg);
    _inputDocsLarge.reserve(kDocsPerMsg);
    _inputDocsXLarge.reserve(kDocsPerMsg);
    _inputDocsXXLarge.reserve(kDocsPerMsg);
    // Generate 10 unique docs and duplicate them as many times as needed.
    for (int i = 0; i < 10; i++) {
        auto smallObj = generateDoc(10);  // BSONObject size of .9KB
        _inputDocsSmall.emplace_back(tojson(std::move(smallObj)));

        auto mediumObj = generateDoc(100);  // BSONObject size of 2.7KB
        _inputDocsMedium.emplace_back(tojson(mediumObj));
        _dataMsg.docs.emplace_back(Document(mediumObj));
        _inputObjs.emplace_back(std::move(mediumObj));

        auto largeObj = generateDoc(1000);  // BSONObject size of 20KB
        _inputDocsLarge.emplace_back(tojson(std::move(largeObj)));

        auto xLargeObj = generateDoc(10'000);  // BSONObject size of 200KB
        _inputDocsXLarge.emplace_back(tojson(std::move(xLargeObj)));

        auto xxLargeObj = generateDoc(100'000);  // BSONObject size of 2MB
        _inputDocsXXLarge.emplace_back(tojson(std::move(xxLargeObj)));
    }
    for (int i = 10; i < kDocsPerMsg; i += 10) {
        std::copy_n(_dataMsg.docs.begin(), 10, std::back_inserter(_dataMsg.docs));
        std::copy_n(_inputObjs.begin(), 10, std::back_inserter(_inputObjs));
        std::copy_n(_inputDocsSmall.begin(), 10, std::back_inserter(_inputDocsSmall));
        std::copy_n(_inputDocsMedium.begin(), 10, std::back_inserter(_inputDocsMedium));
        std::copy_n(_inputDocsLarge.begin(), 10, std::back_inserter(_inputDocsLarge));
        std::copy_n(_inputDocsXLarge.begin(), 10, std::back_inserter(_inputDocsXLarge));
        std::copy_n(_inputDocsXXLarge.begin(), 10, std::back_inserter(_inputDocsXXLarge));
    }
    invariant(_inputObjs.size() == kDocsPerMsg);
    invariant(_inputDocsSmall.size() == kDocsPerMsg);
    invariant(_inputDocsMedium.size() == kDocsPerMsg);
    invariant(_inputDocsLarge.size() == kDocsPerMsg);
    invariant(_inputDocsXLarge.size() == kDocsPerMsg);
    invariant(_inputDocsXXLarge.size() == kDocsPerMsg);
    invariant(_dataMsg.docs.size() == kDocsPerMsg);
}

void OperatorDagBMFixture::TearDown(benchmark::State& state) {
    _dataMsg = StreamDataMsg{};
    _inputObjs.clear();
    _inputDocsSmall.clear();
    _inputDocsMedium.clear();
    _inputDocsLarge.clear();
    _inputDocsXLarge.clear();
    _inputDocsXXLarge.clear();
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

BSONObj OperatorDagBMFixture::generateBSONObj(int numFields, int stringFieldSize) {
    BSONObjBuilder objBuilder;

    int numIntFields = numFields / 2;
    for (int i = 0; i < numIntFields; ++i) {
        objBuilder.append(fmt::format("intField{}", i), _random.nextInt64(/*max*/ 1000));
    }
    int numStrFields = numFields / 2;
    for (int i = 0; i < numStrFields; ++i) {
        objBuilder.append(fmt::format("strField{}", i), generateRandomString(stringFieldSize));
    }
    return objBuilder.obj();
}

BSONObj OperatorDagBMFixture::generateDoc(int stringFieldSize) {
    BSONObjBuilder objBuilder(generateBSONObj(20, stringFieldSize));
    objBuilder.append("subObj", generateBSONObj(20, stringFieldSize));
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
        Planner planner(context.get(), {});
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

void OperatorDagBMFixture::runDeserializerBenchmark(benchmark::State& state,
                                                    JsonEventDeserializer deserializer,
                                                    const std::vector<std::string>& inputDocs) {
    invariant(inputDocs.size() == kDocsPerMsg);
    std::vector<BSONObj> results(kDocsPerMsg);

    // Run the benchmark.
    for (auto keepRunning : state) {
        for (int i = 0; i < 10; ++i) {
            for (int j = 0; j < kDocsPerMsg; ++j) {
                const auto& doc = inputDocs[j];
                results[j] = deserializer.deserialize(doc.c_str(), doc.size());
            }
        }
    }
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializerSmallDocs)(benchmark::State& state) {
    runDeserializerBenchmark(state, JsonEventDeserializer(), _inputDocsSmall);
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializerMediumDocs)(benchmark::State& state) {
    runDeserializerBenchmark(state, JsonEventDeserializer(), _inputDocsMedium);
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializerLargeDocs)(benchmark::State& state) {
    runDeserializerBenchmark(state, JsonEventDeserializer(), _inputDocsLarge);
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializerXLargeDocs)(benchmark::State& state) {
    runDeserializerBenchmark(state, JsonEventDeserializer(), _inputDocsXLarge);
}

BENCHMARK_F(OperatorDagBMFixture, BM_JsonDeserializerXXLargeDocs)(benchmark::State& state) {
    runDeserializerBenchmark(state, JsonEventDeserializer(), _inputDocsXXLarge);
}

}  // namespace
}  // namespace mongo
