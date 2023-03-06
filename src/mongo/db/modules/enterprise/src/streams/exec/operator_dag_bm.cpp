/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/in_memory_source_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator_dag.h"
#include <benchmark/benchmark.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <chrono>
#include <exception>
#include <functional>
#include <string>

#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/unittest/unittest.h"

#include "streams/exec/document_source_wrapper_operator.h"
#include "streams/exec/parser.h"

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

    void parse(benchmark::State& state, bool execute, const std::string& userPipeline) {
        const NamespaceString kNss{"operatordagtest.bm"};
        QueryTestServiceContext serviceContext;
        auto opCtx = serviceContext.makeOperationContext();
        auto expCtx = make_intrusive<ExpressionContextForTest>(opCtx.get(), kNss);

        const auto inputBson = fromjson("{pipeline: " + userPipeline + "}");
        auto userPipelineVector = parsePipelineFromBSON(inputBson["pipeline"]);

        for (auto keepRunning : state) {
            // Create a streaming DAG from the user JSON
            Parser parser;
            std::unique_ptr<OperatorDag> dag(parser.fromBson(expCtx, userPipelineVector));

            // Cast to get rid of const-ness.
            auto& operators = const_cast<OperatorDag::OperatorContainer&>(dag->operators());
            // Add an in-memory source.
            auto first = operators.front().get();
            operators.insert(
                operators.begin(),
                std::make_unique<InMemorySourceSinkOperator>(/*numInputs*/ 0, /*numOutputs*/ 1));
            auto source = dynamic_cast<InMemorySourceSinkOperator*>(operators.front().get());
            source->addOutput(first, 0);
            // Add a in-memory sink.
            auto last = operators.back().get();
            operators.push_back(
                std::make_unique<InMemorySourceSinkOperator>(/*numInputs*/ 1, /*numOutputs*/ 0));
            auto sink = dynamic_cast<InMemorySourceSinkOperator*>(operators.back().get());
            last->addOutput(sink, 0);

            // Start the dag.
            dag->start();

            if (execute) {
                for (auto& dataMsg : _dataMsgs) {
                    source->addDataMsg(dataMsg, boost::none);
                    source->runOnce();
                }

                // Verify the correct output.
                auto& opMessages = sink->getMessages();
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
