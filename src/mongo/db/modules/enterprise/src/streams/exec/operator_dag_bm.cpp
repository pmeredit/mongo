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
#include "mongo/db/exec/sbe/expressions/runtime_environment.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/db/query/bson_typemask.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/query/plan_executor_factory.h"
#include "mongo/platform/random.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
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
    // Enum for doc sizes used by benchmarks in this file.
    enum class DocSize {
        kSmallDoc = 0,
        kMediumDoc = 1,
        kLargeDoc = 2,
        kXLargeDoc = 3,
        kXXLargeDoc = 4,
        kNumDocSizes = 5,
    };

    // Enum for doc kinds/structures used by benchmarks in this file.
    enum class DocKind {
        kDocKind1 = 0,
        kDocKind2 = 1,
        kNumDocSizes = 2,
    };

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

    // Generates a doc of kDocKind1 kind.
    BSONObj generateDocKind1(int stringFieldSize);
    // Generates a doc of kDocKind2 kind.
    BSONObj generateDocKind2(int stringFieldSize);

    static DocKind kDataMsgDocKind;
    static DocSize kDataMsgDocSize;
    static size_t kNumInputDocs;
    static size_t kDocsPerMsg;

    PseudoRandom _random;
    std::unique_ptr<MetricManager> _metricManager;

    std::vector<BSONObj> _addFieldsObjs;
    std::vector<BSONObj> _matchObjs;
    std::vector<BSONObj> _projectObjs;
    BSONObj _tumblingWindowObj;
    BSONObj _groupObj;
    StreamDataMsg _dataMsg;
    std::vector<BSONObj> _inputObjs;
    std::vector<std::vector<std::string>> _inputDocs;
    std::vector<BSONObj> _spPipelines;
    std::vector<BSONObj> _aggPipelines;
};

OperatorDagBMFixture::DocKind OperatorDagBMFixture::kDataMsgDocKind{DocKind::kDocKind1};
OperatorDagBMFixture::DocSize OperatorDagBMFixture::kDataMsgDocSize{DocSize::kMediumDoc};
size_t OperatorDagBMFixture::kNumInputDocs{100'000};
size_t OperatorDagBMFixture::kDocsPerMsg{1000};

OperatorDagBMFixture::OperatorDagBMFixture()
    : _random(/*seed*/ 1), _metricManager(std::make_unique<MetricManager>()) {
    _addFieldsObjs.resize(6);
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

    _matchObjs.resize(3);
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

    _projectObjs.resize(1);
    _projectObjs[0] = fromjson(R"(
        { $project: {
            strField: { $concat: [ "$strField", "!" ] }
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

    _spPipelines.reserve(3);
    // The only difference between pipeline#1 and pipeline#2 is that pipeline#1 uses isNumber
    // function.
    _spPipelines.push_back(
        BSON("pipeline" << BSON_ARRAY(getTestSourceSpec()
                                      << _addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                      << _addFieldsObjs[3] << _addFieldsObjs[4] << _addFieldsObjs[5]
                                      << _matchObjs[2] << _matchObjs[1] << _tumblingWindowObj
                                      << getTestMemorySinkSpec())));
    _spPipelines.push_back(
        BSON("pipeline" << BSON_ARRAY(getTestSourceSpec()
                                      << _addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                      << _addFieldsObjs[3] << _addFieldsObjs[4] << _addFieldsObjs[5]
                                      << _matchObjs[0] << _matchObjs[1] << _tumblingWindowObj
                                      << getTestMemorySinkSpec())));
    // pipeline#3 does not use a window stage.
    _spPipelines.push_back(
        BSON("pipeline" << BSON_ARRAY(getTestSourceSpec()
                                      << _projectObjs[0] << _projectObjs[0] << _projectObjs[0]
                                      << _projectObjs[0] << _projectObjs[0] << _projectObjs[0]
                                      << _projectObjs[0] << getNoOpSinkSpec())));

    _aggPipelines.reserve(2);
    _aggPipelines.push_back(
        BSON("pipeline" << BSON_ARRAY(_addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                                        << _addFieldsObjs[3] << _addFieldsObjs[4]
                                                        << _addFieldsObjs[5] << _matchObjs[2]
                                                        << _matchObjs[1] << _groupObj)));
    _aggPipelines.push_back(
        BSON("pipeline" << BSON_ARRAY(_addFieldsObjs[0] << _addFieldsObjs[1] << _addFieldsObjs[2]
                                                        << _addFieldsObjs[3] << _addFieldsObjs[4]
                                                        << _addFieldsObjs[5] << _matchObjs[0]
                                                        << _matchObjs[1] << _groupObj)));
}

void OperatorDagBMFixture::SetUp(benchmark::State& state) {
    if (state.thread_index == 0) {
        auto service = ServiceContext::make();
        setGlobalServiceContext(std::move(service));
    }

    _dataMsg = StreamDataMsg{};
    _dataMsg.docs.reserve(kDocsPerMsg);
    _inputObjs.reserve(kDocsPerMsg);
    _inputDocs.resize(static_cast<size_t>(DocSize::kNumDocSizes));
    auto& inputDocsSmall = _inputDocs[static_cast<size_t>(DocSize::kSmallDoc)];
    inputDocsSmall.reserve(kDocsPerMsg);
    auto& inputDocsMedium = _inputDocs[static_cast<size_t>(DocSize::kMediumDoc)];
    inputDocsMedium.reserve(kDocsPerMsg);
    auto& inputDocsLarge = _inputDocs[static_cast<size_t>(DocSize::kLargeDoc)];
    inputDocsLarge.reserve(kDocsPerMsg);
    auto& inputDocsXLarge = _inputDocs[static_cast<size_t>(DocSize::kXLargeDoc)];
    inputDocsXLarge.reserve(kDocsPerMsg);
    auto& inputDocsXXLarge = _inputDocs[static_cast<size_t>(DocSize::kXXLargeDoc)];
    inputDocsXXLarge.reserve(kDocsPerMsg);
    // Generate 10 unique docs and duplicate them as many times as needed.
    size_t numUniqueDocs = std::min<size_t>(kDocsPerMsg, 10);
    for (size_t i = 0; i < numUniqueDocs; i++) {
        BSONObj smallObj, mediumObj, largeObj, xLargeObj, xxLargeObj;
        switch (kDataMsgDocKind) {
            case DocKind::kDocKind1: {
                smallObj = generateDocKind1(10);         // BSONObject size of .9KB
                mediumObj = generateDocKind1(100);       // BSONObject size of 2.7KB
                largeObj = generateDocKind1(1000);       // BSONObject size of 20KB
                xLargeObj = generateDocKind1(10'000);    // BSONObject size of 200KB
                xxLargeObj = generateDocKind1(100'000);  // BSONObject size of 2MB
                break;
            }
            case DocKind::kDocKind2: {
                smallObj = generateDocKind2(900);        // BSONObject size of .9KB
                mediumObj = generateDocKind2(2700);      // BSONObject size of 2.7KB
                largeObj = generateDocKind2(20700);      // BSONObject size of 20KB
                xLargeObj = generateDocKind2(200700);    // BSONObject size of 200KB
                xxLargeObj = generateDocKind2(2000700);  // BSONObject size of 2MB
                break;
            }
            default:
                MONGO_UNREACHABLE;
        }

        inputDocsSmall.emplace_back(tojson(smallObj));
        inputDocsMedium.emplace_back(tojson(mediumObj));
        inputDocsLarge.emplace_back(tojson(largeObj));
        inputDocsXLarge.emplace_back(tojson(xLargeObj));
        inputDocsXXLarge.emplace_back(tojson(xxLargeObj));

        boost::optional<StreamDocument> streamDoc;
        switch (kDataMsgDocSize) {
            case DocSize::kSmallDoc: {
                streamDoc = Document(smallObj);
                _inputObjs.emplace_back(std::move(smallObj));
                break;
            }
            case DocSize::kMediumDoc: {
                streamDoc = Document(mediumObj);
                _inputObjs.emplace_back(std::move(mediumObj));
                break;
            }
            case DocSize::kLargeDoc: {
                streamDoc = Document(largeObj);
                _inputObjs.emplace_back(std::move(largeObj));
                break;
            }
            case DocSize::kXLargeDoc: {
                streamDoc = Document(xLargeObj);
                _inputObjs.emplace_back(std::move(xLargeObj));
                break;
            }
            case DocSize::kXXLargeDoc: {
                streamDoc = Document(xxLargeObj);
                _inputObjs.emplace_back(std::move(xxLargeObj));
                break;
            }
            default:
                MONGO_UNREACHABLE;
        }
        streamDoc->minDocTimestampMs = 1000;
        streamDoc->maxDocTimestampMs = 1000;
        _dataMsg.docs.emplace_back(std::move(*streamDoc));
    }

    if (kDocsPerMsg > 10) {
        ASSERT_TRUE(kDocsPerMsg % 10 == 0);
        ASSERT_EQ(10, _inputObjs.size());
        for (size_t i = 10; i < kDocsPerMsg; i += 10) {
            // Ensure that every doc in _dataMsg is an owned doc to mimic the production scenario.
            for (size_t j = 0; j < 10; ++j) {
                StreamDocument streamDoc{_dataMsg.docs[j].doc.getOwned()};
                streamDoc.minDocTimestampMs = 1000;
                streamDoc.maxDocTimestampMs = 1000;
                _dataMsg.docs.emplace_back(std::move(streamDoc));
            }
            std::copy_n(_inputObjs.begin(), 10, std::back_inserter(_inputObjs));
            std::copy_n(inputDocsSmall.begin(), 10, std::back_inserter(inputDocsSmall));
            std::copy_n(inputDocsMedium.begin(), 10, std::back_inserter(inputDocsMedium));
            std::copy_n(inputDocsLarge.begin(), 10, std::back_inserter(inputDocsLarge));
            std::copy_n(inputDocsXLarge.begin(), 10, std::back_inserter(inputDocsXLarge));
            std::copy_n(inputDocsXXLarge.begin(), 10, std::back_inserter(inputDocsXXLarge));
        }
    }
    invariant(_inputObjs.size() == kDocsPerMsg);
    invariant(inputDocsSmall.size() == kDocsPerMsg);
    invariant(inputDocsMedium.size() == kDocsPerMsg);
    invariant(inputDocsLarge.size() == kDocsPerMsg);
    invariant(inputDocsXLarge.size() == kDocsPerMsg);
    invariant(inputDocsXXLarge.size() == kDocsPerMsg);
    invariant(_dataMsg.docs.size() == kDocsPerMsg);
}

void OperatorDagBMFixture::TearDown(benchmark::State& state) {
    _dataMsg = StreamDataMsg{};
    _inputObjs.clear();
    _inputDocs.clear();
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

BSONObj OperatorDagBMFixture::generateDocKind1(int stringFieldSize) {
    BSONObjBuilder objBuilder(generateBSONObj(20, stringFieldSize));
    objBuilder.append("subObj", generateBSONObj(20, stringFieldSize));
    return objBuilder.obj();
}

BSONObj OperatorDagBMFixture::generateDocKind2(int stringFieldSize) {
    BSONObjBuilder objBuilder;
    objBuilder.append("strField", generateRandomString(stringFieldSize));
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
        for (size_t i = 0; i * kDocsPerMsg < kNumInputDocs; i++) {
            // Generate watermarks such that each StreamDataMsg is in its own window.
            for (auto& streamDoc : _dataMsg.docs) {
                streamDoc.minDocTimestampMs = i * 1000;
                streamDoc.maxDocTimestampMs = i * 1000;
            }
            controlMsg.watermarkMsg =
                WatermarkControlMsg{.watermarkTimestampMs = _dataMsg.docs[0].minDocTimestampMs};
            sourcePtr->addDataMsg(_dataMsg, controlMsg);
            sourcePtr->runOnce();
        }
        controlMsg.watermarkMsg->watermarkTimestampMs += 1000;
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
        ASSERT_EQ(totalMessages, kNumInputDocs / kDocsPerMsg);

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

        size_t numDataMsgsSent{0};
        size_t totalMessages = 0;
        auto result = aggPipeline->getSources().back()->getNext();
        while (true) {
            if (result.isAdvanced()) {
                ++totalMessages;
                ASSERT_EQ(result.getDocument()["TotalNumRecords"].getInt(), kNumInputDocs);
            } else if (result.isPaused()) {
                if (numDataMsgsSent < kNumInputDocs / kDocsPerMsg) {
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

BENCHMARK_F(OperatorDagBMFixture, BM_RunStreamProcessorType1)(benchmark::State& state) {
    runStreamProcessor(state, _spPipelines[0]);
}

BENCHMARK_F(OperatorDagBMFixture, BM_RunAggregationPipelineType1)(benchmark::State& state) {
    runAggregationPipeline(state, _aggPipelines[0]);
}

// We cannot run Type1 benchmark for SBE as ABT does not support isNumber function yet.

BENCHMARK_F(OperatorDagBMFixture, BM_RunStreamProcessorType2)(benchmark::State& state) {
    runStreamProcessor(state, _spPipelines[1]);
}

BENCHMARK_F(OperatorDagBMFixture, BM_RunAggregationPipelineType2)(benchmark::State& state) {
    runAggregationPipeline(state, _aggPipelines[1]);
}

void OperatorDagBMFixture::runDeserializerBenchmark(benchmark::State& state,
                                                    JsonEventDeserializer deserializer,
                                                    const std::vector<std::string>& inputDocs) {
    invariant(inputDocs.size() == kDocsPerMsg);
    std::vector<BSONObj> results(kDocsPerMsg);

    // Run the benchmark.
    for (auto keepRunning : state) {
        for (size_t i = 0; i < 10; ++i) {
            for (size_t j = 0; j < kDocsPerMsg; ++j) {
                const auto& doc = inputDocs[j];
                results[j] = deserializer.deserialize(doc.c_str(), doc.size());
            }
        }
    }
}

// A microbenchmark to figure out the impact of DataMsg size on the performance of a stream
// processor.
class BatchSizeBenchmark : public OperatorDagBMFixture {
public:
    BatchSizeBenchmark() : OperatorDagBMFixture() {}

    void SetUp(benchmark::State& state) override {
        size_t argIndex = 0;
        kNumInputDocs = state.range(argIndex++);
        kDataMsgDocSize = static_cast<DocSize>(state.range(argIndex++));
        kDocsPerMsg = state.range(argIndex++);
        kDataMsgDocKind = DocKind::kDocKind2;
        std::cout << "NumInputDocs: " << kNumInputDocs
                  << " DocSize: " << static_cast<int32_t>(kDataMsgDocSize)
                  << " DocKind: " << static_cast<int32_t>(kDataMsgDocKind)
                  << " BatchSize: " << kDocsPerMsg << std::endl;
        OperatorDagBMFixture::SetUp(state);
        std::cout << "Batch byte size: " << _dataMsg.getByteSize() << std::endl;
    }

protected:
    void runBatchSizeBenchmark(benchmark::State& state, const BSONObj& pipelineSpec);
};

void BatchSizeBenchmark::runBatchSizeBenchmark(benchmark::State& state,
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

        // Add an in-memory source.
        auto sourcePtr = dynamic_cast<InMemorySourceOperator*>(dag->source());
        invariant(sourcePtr);

        // Register metrics for all operators.
        for (const auto& oper : dag->operators()) {
            oper->registerMetrics(_metricManager.get());
        }

        // Start the dag.
        dag->start();

        ASSERT_EQ(_dataMsg.docs.size(), kDocsPerMsg);

        StreamDataMsg dataMsg;
        StreamControlMsg controlMsg;
        controlMsg.watermarkMsg = WatermarkControlMsg{};
        for (size_t i = 0; i * kDocsPerMsg < kNumInputDocs; i++) {
            // Generate watermarks such that each StreamDataMsg is in its own window.
            for (auto& streamDoc : _dataMsg.docs) {
                streamDoc.minDocTimestampMs = i * 1000;
                streamDoc.maxDocTimestampMs = i * 1000;
            }
            controlMsg.watermarkMsg =
                WatermarkControlMsg{.watermarkTimestampMs = _dataMsg.docs[0].minDocTimestampMs};
            sourcePtr->addDataMsg(_dataMsg, controlMsg);
            sourcePtr->runOnce();
        }
        controlMsg.watermarkMsg->watermarkTimestampMs += 1000;
        sourcePtr->addControlMsg(std::move(controlMsg));
        sourcePtr->runOnce();
        dag->stop();
    }
}

BENCHMARK_DEFINE_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkSmallDocs)(benchmark::State& state) {
    runBatchSizeBenchmark(state, _spPipelines[2]);
}

BENCHMARK_REGISTER_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkSmallDocs)->Apply([](auto* b) {
    b->ArgNames({"NumInputDocs", "DocSize", "BatchSize"});
    auto docSize = static_cast<int64_t>(OperatorDagBMFixture::DocSize::kSmallDoc);
    b->Args({10000, docSize, 1});
    b->Args({10000, docSize, 10});
    b->Args({10000, docSize, 100});
    b->Args({10000, docSize, 1000});
    b->Args({10000, docSize, 10000});
});

BENCHMARK_DEFINE_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkMediumDocs)(benchmark::State& state) {
    runBatchSizeBenchmark(state, _spPipelines[2]);
}

BENCHMARK_REGISTER_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkMediumDocs)->Apply([](auto* b) {
    b->ArgNames({"NumInputDocs", "DocSize", "BatchSize"});
    auto docSize = static_cast<int64_t>(OperatorDagBMFixture::DocSize::kMediumDoc);
    b->Args({10000, docSize, 1});
    b->Args({10000, docSize, 10});
    b->Args({10000, docSize, 100});
    b->Args({10000, docSize, 1000});
    b->Args({10000, docSize, 10000});
});

BENCHMARK_DEFINE_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkLargeDocs)(benchmark::State& state) {
    runBatchSizeBenchmark(state, _spPipelines[2]);
}

BENCHMARK_REGISTER_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkLargeDocs)->Apply([](auto* b) {
    b->ArgNames({"NumInputDocs", "DocSize", "BatchSize"});
    auto docSize = static_cast<int64_t>(OperatorDagBMFixture::DocSize::kLargeDoc);
    b->Args({10000, docSize, 1});
    b->Args({10000, docSize, 10});
    b->Args({10000, docSize, 100});
    b->Args({10000, docSize, 1000});
    b->Args({10000, docSize, 10000});
});

BENCHMARK_DEFINE_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkXLargeDocs)(benchmark::State& state) {
    runBatchSizeBenchmark(state, _spPipelines[2]);
}

BENCHMARK_REGISTER_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkXLargeDocs)->Apply([](auto* b) {
    b->ArgNames({"NumInputDocs", "DocSize", "BatchSize"});
    auto docSize = static_cast<int64_t>(OperatorDagBMFixture::DocSize::kXLargeDoc);
    b->Args({10000, docSize, 1});
    b->Args({10000, docSize, 10});
    b->Args({10000, docSize, 100});
    b->Args({10000, docSize, 1000});
    b->Args({10000, docSize, 10000});
});

BENCHMARK_DEFINE_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkXXLargeDocs)(benchmark::State& state) {
    runBatchSizeBenchmark(state, _spPipelines[2]);
}

BENCHMARK_REGISTER_F(BatchSizeBenchmark, BM_BatchSizeBenchmarkXXLargeDocs)->Apply([](auto* b) {
    b->ArgNames({"NumInputDocs", "DocSize", "BatchSize"});
    auto docSize = static_cast<int64_t>(OperatorDagBMFixture::DocSize::kXXLargeDoc);
    b->Args({10000, docSize, 1});
    b->Args({10000, docSize, 10});
    b->Args({10000, docSize, 100});
    b->Args({10000, docSize, 1000});
});

}  // namespace
}  // namespace mongo
