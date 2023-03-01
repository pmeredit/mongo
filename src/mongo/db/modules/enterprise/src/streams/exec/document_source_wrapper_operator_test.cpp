/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/bson/bsonelement.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/in_memory_source_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator_dag.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/unittest/unittest.h"

#include "streams/exec/document_source_wrapper_operator.h"
#include "streams/exec/operator_dag_factory.h"

namespace streams {
namespace {

using namespace mongo;

class DocumentSourceWrapperOperatorTest : public AggregationContextFixture {
protected:
    void compareStreamingDagAndPipeline(const std::string& userPipeline,
                                        const std::vector<Document>& input) {
        OperatorDagFactory factory;
        const auto inputBson = fromjson("{pipeline: " + userPipeline + "}");
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);

        // Init the streaming dag
        auto userPipelineVector = parsePipelineFromBSON(inputBson["pipeline"]);
        std::unique_ptr<OperatorDag> dag(factory.fromBson(getExpCtx(), userPipelineVector));
        // Append a sink operator to the dag. Note: if this need keeps coming up we can
        // add a method to OperatorDag to do this.
        auto sink = std::make_unique<InMemorySourceSinkOperator>(/*numInputs*/ 1, /*numOutputs*/ 0);
        auto sinkP = sink.get();
        auto& currentLast = dag->operators().back();
        currentLast->addOutput(sinkP, 0);
        ((OperatorDag::OperatorContainer&)dag->operators()).push_back(std::move(sink));

        // Setup the pipeline with a feeder containing {input}.
        auto pipeline = Pipeline::parse(userPipelineVector, getExpCtx());
        auto feeder = boost::intrusive_ptr<DocumentSourceFeeder>(
            new DocumentSourceFeeder(pipeline->getContext()));
        for (auto& doc : input) {
            feeder->addDocument(doc);
        }
        pipeline->addInitialSource(feeder);

        // Get the pipeline results
        std::vector<Document> pipelineResults;
        auto result = pipeline->getSources().back()->getNext();
        while (result.isAdvanced()) {
            pipelineResults.emplace_back(std::move(result.getDocument()));
            result = pipeline->getSources().back()->getNext();
        }
        dassert(result.isPaused());

        // Get the dag results
        std::vector<StreamDocument> docs;
        for (auto& doc : input) {
            docs.push_back(doc);
        }
        dag->operators().front()->onDataMsg(0, {docs}, boost::none);
        auto& opMessages = sinkP->getMessages();
        std::vector<Document> opResults;
        for (size_t i = 0; i < sinkP->getMessages().size(); i += 1) {
            StreamMsgUnion msg = std::move(opMessages.front());
            ASSERT_TRUE(msg.dataMsg);
            for (auto& doc : msg.dataMsg.value().docs) {
                opResults.push_back(std::move(doc.doc));
            }
        }

        // Compare the results
        ASSERT_EQ(pipelineResults.size(), opResults.size());
        for (size_t i = 0; i < pipelineResults.size(); i++) {
            ASSERT_VALUE_EQ(Value(pipelineResults[i]), Value(opResults[i]));
        }
    }
};

TEST_F(DocumentSourceWrapperOperatorTest, Basic) {

    std::string userPipeline = R"(
[
    { $addFields: { a: 5 } },
    { $match: { b: 5 } },
    { $project: { a: 1, b: 1, sizes: 1, name: 1, o: 1 } },
    { $set: { c: 1 } },
    { $redact: { $cond: { 
        if: { $eq: [ "$a", 5 ] },
        then: "$$KEEP",
        else: "$$PRUNE"
    }}},
    { $set: {p: {a: "hello world"} }},
    { $replaceRoot: { newRoot: "$o" }},
    { $replaceWith: "$p" },
    { $unset: "q" },
    { $unwind: "$sizes" }
]
    )";

    std::vector<Document> input = {
        Document(fromjson("{a: 1, b: 5, name: 'a', o: {p: { q: 1, z: 1, sizes: [1, 2, 3]}}}}")),
        Document(fromjson("{a: 2, b: 2, name: 'b', o: {p: { q: 2, z: 2, sizes: [4, 5]}}}")),
        Document(fromjson("{a: 3, b: 3, name: 'c', o: {p: { q: 3, z: 3, sizes: [6, 7, 8]}}}")),
        Document(fromjson("{a: 4, b: 4, name: 'd', o: {p: { q: 4, z: 4, sizes: [9, 10]}}}")),
        Document(fromjson("{a: 5, b: 5, name: 'e', o: {p: { q: 5, z: 5, sizes: [11]}}}")),
    };

    compareStreamingDagAndPipeline(userPipeline, input);
}

}  // namespace
}  // namespace streams
