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
#include "mongo/unittest/unittest.h"
#include "streams/exec/context.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/document_source_wrapper_operator.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/tests/test_utils.h"

namespace mongo {
namespace {

using namespace streams;
using namespace std;

Document generateDoc(int idx) {
    constexpr int ordersPerCustomer = 10;
    auto arr = BSONArrayBuilder()
                   .append(BSON("sku"
                                << "a"
                                << "cost" << 123.0 << "quantity" << 2))
                   .append(BSON("sku"
                                << "b"
                                << "cost" << 123.5 << "quantity" << 3))
                   .arr();
    auto bson = BSON("customerId" << idx / ordersPerCustomer << "orderId" << idx % ordersPerCustomer
                                  << "products" << arr);
    return Document(bson);
}

class OperatorDagBMFixture : public benchmark::Fixture {
public:
    void SetUp(benchmark::State& state) override {
        _context = getTestContext();
        _simpleInput.clear();
        _dataMsgs.clear();
        constexpr int numDocs = 10'000;
        for (int i = 0; i < numDocs; i++) {
            _simpleInput.push_back(generateDoc(i));
        }

        // Break the input up into chunks of StreamDataMsg.
        constexpr int docsPerMsg = 1000;
        size_t i = 0;
        while (i < _simpleInput.size()) {
            _dataMsgs.push_back(StreamDataMsg{});
            auto j = 0;
            while (j < docsPerMsg && i < _simpleInput.size()) {
                _dataMsgs.back().docs.emplace_back(_simpleInput[i]);
                i += 1;
                j += 1;
            }
        }
    }

    void TearDown(benchmark::State& state) override {
        _simpleInput.clear();
    }

    void parse(benchmark::State& state, bool execute, const std::string& bsonPipeline) {
        const NamespaceString kNss{"operatordagtest.bm"};
        QueryTestServiceContext serviceContext;

        const auto inputBson = fromjson("{pipeline: " + bsonPipeline + "}");
        auto bsonPipelineVector = parsePipelineFromBSON(inputBson["pipeline"]);

        for (auto keepRunning : state) {
            // Create a streaming DAG from the user JSON
            Parser parser(_context.get(), {});
            std::unique_ptr<OperatorDag> dag(parser.fromBson(bsonPipelineVector));

            // Add an in-memory source.
            auto source = std::make_unique<InMemorySourceOperator>(/*numOutputs*/ 1);
            auto sourcePtr = source.get();
            source->addOutput(dag->source(), 0);
            dag->pushFront(std::move(source));
            // Add a in-memory sink.
            auto sink = std::make_unique<InMemorySinkOperator>(/*numInputs*/ 1);
            auto sinkPtr = sink.get();
            dag->sink()->addOutput(sink.get(), 0);
            dag->pushBack(std::move(sink));

            // Start the dag.
            dag->start();

            if (execute) {
                for (auto& dataMsg : _dataMsgs) {
                    sourcePtr->addDataMsg(dataMsg, boost::none);
                    sourcePtr->runOnce();
                }

                // Verify the correct output.
                auto opMessages = sinkPtr->getMessages();
                size_t totalMessages = 0;
                while (!opMessages.empty()) {
                    const auto& msg = opMessages.front();
                    totalMessages += msg.dataMsg->docs.size();
                    opMessages.pop();
                }
                ASSERT_EQ(totalMessages, _simpleInput.size() * 2);
            }
        }
    }

protected:
    std::unique_ptr<Context> _context;
    std::vector<Document> _simpleInput;
    std::vector<StreamDataMsg> _dataMsgs;
    const std::string _simplePipeline = R"([
        { $unwind: "$products" },
        { $project: { 
            orderId: 1, 
            customerId: 1, 
            totalCost: { $multiply: [ "$products.cost", "$products.quantity" ] } 
        }},
        { $project: { 
            orderId: 1, 
            customerId: 1, 
            totalCostNative: "$totalCost",
            totalCostUSD: {
                $cond: { 
                    if: { $eq: [ "$sku", "a" ] }, 
                    then: "$totalCost", 
                    else: { $divide: [ "$totalCost", 14.59 ] } 
                }
            }
        }}
    ])";
};

BENCHMARK_F(OperatorDagBMFixture, BM_Parse)(benchmark::State& state) {
    parse(state, false /*execute*/, _simplePipeline);
}

BENCHMARK_F(OperatorDagBMFixture, BM_ParseAndExecute10ThousandDocs)(benchmark::State& state) {
    parse(state, true /*execute*/, _simplePipeline);
}

}  // namespace
}  // namespace mongo
