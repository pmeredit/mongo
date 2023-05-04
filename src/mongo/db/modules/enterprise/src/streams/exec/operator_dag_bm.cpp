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
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/platform/random.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/context.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/document_source_wrapper_operator.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/test_utils.h"

namespace mongo {
namespace {

using namespace streams;
using namespace std;

class OperatorDagBMFixture : public benchmark::Fixture {
public:
    OperatorDagBMFixture() : _random(/*seed*/ 1) {}

    void SetUp(benchmark::State& state) override {
        _dataMsg = StreamDataMsg{};
        _dataMsg.docs.reserve(kDocsPerMsg);
        invariant(kDocsPerMsg % 10 == 0);
        // Generate 10 unique docs and duplicate them as many times as needed.
        for (int i = 0; i < 10; i++) {
            _dataMsg.docs.emplace_back(generateDoc());
        }
        for (int i = 10; i < kDocsPerMsg; i += 10) {
            std::copy_n(_dataMsg.docs.begin(), 10, std::back_inserter(_dataMsg.docs));
        }
        invariant(_dataMsg.docs.size() == kDocsPerMsg);
    }

    void TearDown(benchmark::State& state) override {
        _dataMsg = StreamDataMsg{};
    }

    void runStreamProcessor(benchmark::State& state, const std::string& bsonPipeline);
    void runAggregationPipeline(benchmark::State& state, const std::string& bsonPipeline);

protected:
    std::string generateRandomString(size_t size);

    BSONObj generateBSONObj(int numFields);

    // Generates a document. Each document has 200 fields in it and is ~13KB in size.
    Document generateDoc();

    static constexpr int32_t kNumDataMsgs{100};
    static constexpr int32_t kDocsPerMsg{1000};
    PseudoRandom _random;
    StreamDataMsg _dataMsg;
};

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
    int numiStrFields = numFields / 2;
    for (int i = 0; i < numiStrFields; ++i) {
        objBuilder.append(fmt::format("strField{}", i), generateRandomString(/*size*/ 100));
    }
    return objBuilder.obj();
}

Document OperatorDagBMFixture::generateDoc() {
    BSONObjBuilder objBuilder(generateBSONObj(100));
    objBuilder.append("subObj", generateBSONObj(100));
    return Document(objBuilder.obj());
}

void OperatorDagBMFixture::runStreamProcessor(benchmark::State& state,
                                              const std::string& bsonPipeline) {
    QueryTestServiceContext qtServiceContext;
    auto svcCtx = qtServiceContext.getServiceContext();
    auto context = getTestContext(svcCtx);

    const auto inputBson = fromjson("{pipeline: " + bsonPipeline + "}");
    auto bsonPipelineVector = parsePipelineFromBSON(inputBson["pipeline"]);

    for (auto keepRunning : state) {
        // Create a streaming DAG from the user JSON
        Parser parser(context.get(), {});
        std::unique_ptr<OperatorDag> dag(parser.fromBson(bsonPipelineVector));
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
            opMessages.pop();
        }
        ASSERT_EQ(totalMessages, kNumDataMsgs);

        dag->stop();
    }
}

void OperatorDagBMFixture::runAggregationPipeline(benchmark::State& state,
                                                  const std::string& bsonPipeline) {
    QueryTestServiceContext qtServiceContext;
    auto svcCtx = qtServiceContext.getServiceContext();
    auto context = getTestContext(svcCtx);

    const auto inputBson = fromjson("{pipeline: " + bsonPipeline + "}");
    auto bsonPipelineVector = parsePipelineFromBSON(inputBson["pipeline"]);

    for (auto keepRunning : state) {
        auto aggPipeline = Pipeline::parse(bsonPipelineVector, context->expCtx);
        aggPipeline->optimizePipeline();

        auto feeder = boost::intrusive_ptr<DocumentSourceFeeder>(
            new DocumentSourceFeeder(aggPipeline->getContext()));
        feeder->setEndOfBufferSignal(DocumentSource::GetNextResult::makeEOF());
        for (int i = 0; i < kNumDataMsgs; i++) {
            for (auto& streamDoc : _dataMsg.docs) {
                feeder->addDocument(streamDoc.doc);
            }
        }
        aggPipeline->addInitialSource(feeder);

        invariant(aggPipeline->getSources().size() == 9);
        std::cout << "Stages in the pipeline: ";
        for (const auto& stage : aggPipeline->getSources()) {
            std::cout << stage->getSourceName() << " -> ";
        }
        std::cout << std::endl;

        size_t totalMessages = 0;
        auto result = aggPipeline->getSources().back()->getNext();
        while (result.isAdvanced()) {
            ++totalMessages;
            result = aggPipeline->getSources().back()->getNext();
        }
        ASSERT(result.isEOF());
        ASSERT_EQ(totalMessages, 1);
    }
}

BENCHMARK_F(OperatorDagBMFixture, BM_RunStreamProcessor)(benchmark::State& state) {
    const std::string bsonPipeline = R"([
        { $source: {
            connectionName: "__testMemory"
        }},
        { $addFields: {
            multField1: { $multiply: [ "$intField0", "$intField1" ] }
        }},
        { $addFields: {
            multField2: { $multiply: [ "$intField2", "$intField3" ] }
        }},
        { $addFields: {
            multField3: { $multiply: [ "$intField4", "$intField5" ] }
        }},
        { $addFields: {
            multField4: { $multiply: [ "$subObj.intField0", "$subObj.intField1" ] }
        }},
        { $addFields: {
            multField5: { $multiply: [ "$subObj.intField2", "$subObj.intField3" ] }
        }},
        { $addFields: {
            multField6: { $multiply: [ "$subObj.intField4", "$subObj.intField5" ] }
        }},
        { $match: {
            $and: [
                { $expr: { $isNumber: "$intField6" } },
                { $expr: { $isNumber: "$intField7" } },
                { $expr: { $isNumber: "$subObj.intField6" } },
                { $expr: { $isNumber: "$subObj.intField7" } }
            ]
        }},
        { $match: {
            $and: [
                { intField8: { $gte: 0 } },
                { intField9: { $gte: 0 } },
                { "subObj.intField8": { $gte: 0 } },
                { "subObj.intField9": { $gte: 0 } }
            ]
        }},
        { $tumblingWindow: {
            interval: {size: NumberInt(100), unit: "ms"},
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
        }},
        { $emit: {
            connectionName: "__testMemory"
        }}
    ])";
    runStreamProcessor(state, bsonPipeline);
}

BENCHMARK_F(OperatorDagBMFixture, BM_RunAggregationPipeline)(benchmark::State& state) {
    const std::string bsonPipeline = R"([
        { $addFields: {
            multField1: { $multiply: [ "$intField0", "$intField1" ] }
        }},
        { $addFields: {
            multField2: { $multiply: [ "$intField2", "$intField3" ] }
        }},
        { $addFields: {
            multField3: { $multiply: [ "$intField4", "$intField5" ] }
        }},
        { $addFields: {
            multField4: { $multiply: [ "$subObj.intField0", "$subObj.intField1" ] }
        }},
        { $addFields: {
            multField5: { $multiply: [ "$subObj.intField2", "$subObj.intField3" ] }
        }},
        { $addFields: {
            multField6: { $multiply: [ "$subObj.intField4", "$subObj.intField5" ] }
        }},
        { $match: {
            $and: [
                { $expr: { $isNumber: "$intField6" } },
                { $expr: { $isNumber: "$intField7" } },
                { $expr: { $isNumber: "$subObj.intField6" } },
                { $expr: { $isNumber: "$subObj.intField7" } }
            ]
        }},
        { $match: {
            $and: [
                { intField8: { $gte: 0 } },
                { intField9: { $gte: 0 } },
                { "subObj.intField8": { $gte: 0 } },
                { "subObj.intField9": { $gte: 0 } }
            ]
        }},
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
        }}
    ])";
    runAggregationPipeline(state, bsonPipeline);
}

}  // namespace
}  // namespace mongo
