/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <exception>
#include <fmt/format.h>
#include <memory>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/constants.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;
using namespace std;

class DocumentTransformationOperatorTest : public AggregationContextFixture {
protected:
    DocumentTransformationOperatorTest() {
        _metricManager = std::make_unique<MetricManager>();
        std::unique_ptr<Executor> _executor;
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
        _context->dlq->registerMetrics(_executor->getMetricManager());
    }

    std::vector<Document> getAggregationPipelineResults(const string& bsonPipeline,
                                                        const vector<StreamDocument>& streamDocs) {
        // Get the user pipeline vector
        const auto inputBson = fromjson("{pipeline: " + bsonPipeline + "}");
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
        auto bsonPipelineVector = parsePipelineFromBSON(inputBson["pipeline"]);

        // Setup the pipeline with a feeder containing {streamDocs}.
        auto pipeline = Pipeline::parse(bsonPipelineVector, getExpCtx());
        auto feeder = boost::intrusive_ptr<DocumentSourceFeeder>(
            new DocumentSourceFeeder(pipeline->getContext()));
        for (auto& streamDoc : streamDocs) {
            feeder->addDocument(streamDoc.doc);
        }
        pipeline->addInitialSource(feeder);

        // Get the pipeline results
        std::vector<Document> pipelineResults;
        auto result = pipeline->getSources().back()->getNext();
        while (result.isAdvanced()) {
            pipelineResults.emplace_back(std::move(result.getDocument()));
            result = pipeline->getSources().back()->getNext();
        }
        ASSERT_TRUE(result.isPaused());
        return pipelineResults;
    }

    std::vector<Document> getStreamingPipelineResults(const string& bsonPipeline,
                                                      const vector<StreamDocument>& streamDocs,
                                                      boost::optional<size_t> expectedNumDlqDocs) {
        _context->connections = testInMemoryConnectionRegistry();
        Planner planner(_context.get(), {});

        // Get the user pipeline vector
        const auto inputBson = fromjson("{pipeline: " + bsonPipeline + "}");
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
        auto bsonPipelineVector = parsePipelineFromBSON(inputBson["pipeline"]);

        // Setup the streaming DAG
        // Add a test source
        bsonPipelineVector.insert(bsonPipelineVector.begin(), getTestSourceSpec());
        // Add a test sink
        bsonPipelineVector.push_back(getTestMemorySinkSpec());
        // Init the streaming dag
        std::unique_ptr<OperatorDag> dag(planner.plan(bsonPipelineVector));
        dag->start();

        // Append a sink operator to the dag. Note: if this need keeps coming up we can
        // add a method to OperatorDag to do this.
        auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

        // Start the streaming dag with the input
        auto source = dynamic_cast<InMemorySourceOperator*>(dag->source());
        source->addDataMsg({streamDocs}, boost::none);
        source->runOnce();

        // Get the dag results
        auto opMessages = sink->getMessages();
        std::vector<Document> opResults;
        while (!opMessages.empty()) {
            StreamMsgUnion msg = std::move(opMessages.front());
            opMessages.pop_front();
            ASSERT_TRUE(msg.dataMsg);
            for (auto& doc : msg.dataMsg.value().docs) {
                opResults.push_back(std::move(doc.doc));
            }
        }

        auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
        ASSERT_EQ(dlq->numMessages(), getNumDlqDocsFromOperatorDag(*dag));
        ASSERT_EQ(dlq->numMessageBytes(), getNumDlqBytesFromOperatorDag(*dag));
        if (expectedNumDlqDocs) {
            ASSERT_EQ(dlq->numMessages(), *expectedNumDlqDocs);
        }

        dag->stop();
        return opResults;
    }

    void compareStreamingDagAndPipeline(const string& bsonPipeline,
                                        const vector<StreamDocument>& streamDocs,
                                        boost::optional<size_t> expectedNumDlqDocs = boost::none) {
        auto pipelineResults = getAggregationPipelineResults(bsonPipeline, streamDocs);
        auto opResults = getStreamingPipelineResults(bsonPipeline, streamDocs, expectedNumDlqDocs);

        // Compare the results
        ASSERT_EQ(pipelineResults.size(), opResults.size());
        for (size_t i = 0; i < pipelineResults.size(); i++) {
            MutableDocument sanitizedDoc(opResults[i]);

            // Remove timestamp injected fields before comparing.
            sanitizedDoc.remove("_ts");
            sanitizedDoc.remove("_stream_meta");

            ASSERT_VALUE_EQ(Value(pipelineResults[i]), Value(sanitizedDoc.freeze()));
        }
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
    const std::vector<StreamDocument> _streamDocs = {
        Document(fromjson("{a: 1, b: 5, name: 'a', o: {p: { q: 1, z: 1, sizes: [1, 2, 3]}}}}")),
        Document(fromjson("{a: 2, b: 2, name: 'b', o: {p: { q: 2, z: 2, sizes: [4, 5]}}}")),
        Document(fromjson("{a: 3, b: 3, name: 'c', o: {p: { q: 3, z: 3, sizes: [6, 7, 8]}}}")),
        Document(fromjson("{a: 4, b: 4, name: 'd', o: {p: { q: 4, z: 4, sizes: [9, 10]}}}")),
        Document(fromjson("{a: 5, b: 5, name: 'e', o: {p: { q: 5, z: 5, sizes: [11]}}}")),
    };
};

TEST_F(DocumentTransformationOperatorTest, FromString) {

    std::string bsonPipeline = R"(
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

    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, AddFields) {
    std::string bsonPipeline = R"(
[
    { $addFields: { c: { $multiply: [5, "$a", "$b"] } } }
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, AddFieldsDlq) {

    StreamDocument streamDoc(Document(fromjson("{a: 1, b: 0}")));
    StreamMetaSource streamMetaSource;
    streamMetaSource.setType(StreamMetaSourceTypeEnum::Kafka);
    streamMetaSource.setPartition(1);
    streamMetaSource.setOffset(10);
    streamDoc.streamMeta.setSource(streamMetaSource);
    std::vector<StreamDocument> streamDocs = {
        streamDoc, streamDoc, streamDoc, streamDoc, streamDoc, streamDoc};
    std::string bsonPipeline = R"(
[
    { $addFields: { c: { $divide: ["$a", "$b"] } } }
]
    )";
    auto docs = getStreamingPipelineResults(bsonPipeline, streamDocs, 6);
    ASSERT_TRUE(docs.empty());
}

TEST_F(DocumentTransformationOperatorTest, Match) {
    std::string bsonPipeline = R"(
[
    { $match: { a: 5 } }
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, Project) {
    std::string bsonPipeline = R"(
[
    { $project: { sizes: 1 } }
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);

    bsonPipeline = R"(
[
    { $unwind: "$sizes" }
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, Redact) {
    std::string bsonPipeline = R"(
[
    { $redact: { $cond: { 
        if: { $eq: [ "$a", 5 ] },
        then: "$$KEEP",
        else: "$$PRUNE"
    }}}
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, ReplaceRoot) {
    std::string bsonPipeline = R"(
[
    { $replaceRoot: { newRoot: "$o" }}
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);

    bsonPipeline = R"(
[
    { $replaceWith: "$o" }
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, Set) {
    std::string bsonPipeline = R"(
[
    { $set: {p: {a: "hello world"} }}
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, Unwind) {
    std::string bsonPipeline = R"(
[
    { $unwind: "$sizes" }
]
    )";
    compareStreamingDagAndPipeline(bsonPipeline, _streamDocs);
}

TEST_F(DocumentTransformationOperatorTest, InvalidOutputs) {
    auto ds = DocumentSourceMatch::create(BSONObj(BSON("a" << 1)), getExpCtx());
    MatchOperator::Options options{.documentSource = ds.get()};
    MatchOperator op(_context.get(), std::move(options));
    ASSERT_THROWS_CODE(op.start(), DBException, ErrorCodes::InternalError);
}

TEST_F(DocumentTransformationOperatorTest, DeadLetterQueue) {
    StreamDocument streamDoc(Document(fromjson("{a: 1, b: 0}")));
    StreamMetaSource streamMetaSource;
    streamMetaSource.setType(StreamMetaSourceTypeEnum::Generated);
    streamMetaSource.setPartition(1);
    streamMetaSource.setOffset(10);
    streamDoc.streamMeta.setSource(streamMetaSource);
    std::vector<StreamDocument> streamDocs = {streamDoc, streamDoc, streamDoc, streamDoc};
    std::string bsonPipeline = R"(
[
    { $project: { sizes: { $divide: [ "$a", "$b" ] } } }
]
    )";

    std::ignore = getStreamingPipelineResults(bsonPipeline, streamDocs, 4);
    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    ASSERT_EQ(4, dlqMsgs.size());
    auto dlqDoc = std::move(dlqMsgs.front());
    ASSERT_EQ(
        "Failed to process input document in ProjectOperator with error: can't $divide by zero",
        dlqDoc["errInfo"]["reason"].String());

    ASSERT_BSONOBJ_EQ(streamDocs[0].streamMeta.toBSON(), dlqDoc["_stream_meta"].Obj());
    ASSERT_EQ("ProjectOperator", dlqDoc["operatorName"].String());
}

}  // namespace
}  // namespace streams
