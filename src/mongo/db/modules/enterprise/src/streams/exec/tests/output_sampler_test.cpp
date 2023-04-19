/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/output_sampler.h"
#include "streams/exec/parser.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

class OutputSamplerTest : public AggregationContextFixture {
public:
    OutputSamplerTest() : _context(getTestContext()) {}

    const std::vector<boost::intrusive_ptr<OutputSampler>>& getSinkSamplers(
        InMemorySinkOperator* sink) {
        return sink->_outputSamplers;
    }

protected:
    std::unique_ptr<Context> _context;
};

TEST_F(OutputSamplerTest, Basic) {
    std::vector<BSONObj> rawPipeline{getTestSourceSpec(), getTestMemorySinkSpec()};

    Parser parser(_context.get(), {});
    std::unique_ptr<OperatorDag> dag = parser.fromBson(rawPipeline);
    dag->start();

    const auto& ops = dag->operators();
    ASSERT_EQ(ops.size(), 2);
    auto source = dynamic_cast<InMemorySourceOperator*>(ops[0].get());
    dassert(source);
    auto sink = dynamic_cast<InMemorySinkOperator*>(ops[1].get());
    dassert(sink);

    // Add 2 samplers to the sink.
    OutputSampler::Options options;
    options.maxDocsToSample = 10;
    options.maxBytesToSample = 50 * (1 << 20);  // 50MB
    auto sampler1 = make_intrusive<OutputSampler>(options);
    options.maxDocsToSample = 15;
    auto sampler2 = make_intrusive<OutputSampler>(options);

    sink->addOutputSampler(sampler1);
    sink->addOutputSampler(sampler2);
    ASSERT_EQUALS(2, getSinkSamplers(sink).size());

    std::vector<BSONObj> inputDocs;
    auto addDocsToSource = [&]() {
        for (int i = 0; i < 5; ++i) {
            StreamDataMsg dataMsg;
            inputDocs.push_back(fromjson(fmt::format("{{a: {}}}", i * 2)));
            dataMsg.docs.emplace_back(Document(inputDocs.back()));
            inputDocs.push_back(fromjson(fmt::format("{{a: {}}}", i * 2 + 1)));
            dataMsg.docs.emplace_back(Document(inputDocs.back()));
            source->addDataMsg(dataMsg);
        }
    };

    // Add 10 docs to source and send them through the OperatorDag.
    addDocsToSource();
    source->runOnce();
    ASSERT_EQUALS(1, getSinkSamplers(sink).size());

    // Fetch all docs in sampler2 while it is still not done sampling.
    ASSERT_FALSE(sampler2->doneSampling());
    {
        int i{0};
        auto docs = sampler2->getNext(/*batchSize*/ 500);
        ASSERT_EQUALS(10, docs.size());
        for (auto& doc : docs) {
            ASSERT_BSONOBJ_EQ(inputDocs[i], doc);
            ++i;
        }
    }
    ASSERT_TRUE(sampler2->getNext(/*batchSize*/ 1).empty());

    // Add one more sampler to the sink.
    options.maxDocsToSample = 5;
    auto sampler3 = make_intrusive<OutputSampler>(options);
    sink->addOutputSampler(sampler3);
    ASSERT_EQUALS(2, getSinkSamplers(sink).size());

    // Add 10 more docs to source and send them through the OperatorDag.
    addDocsToSource();
    source->runOnce();
    ASSERT_TRUE(getSinkSamplers(sink).empty());

    // Verify that all samplers are done and they got the correct set of output docs.
    ASSERT_TRUE(sampler1->doneSampling());
    int i{0};
    while (i < 10) {
        auto docs = sampler1->getNext(/*batchSize*/ 3);
        ASSERT_TRUE(docs.size() <= 3);
        for (auto& doc : docs) {
            ASSERT_BSONOBJ_EQ(inputDocs[i], doc);
            ++i;
        }
    }
    ASSERT_TRUE(sampler1->getNext(/*batchSize*/ 3).empty());

    ASSERT_TRUE(sampler2->doneSampling());
    i = 10;
    while (i < 15) {
        auto docs = sampler2->getNext(/*batchSize*/ 3);
        ASSERT_TRUE(docs.size() <= 3);
        for (auto& doc : docs) {
            ASSERT_BSONOBJ_EQ(inputDocs[i], doc);
            ++i;
        }
    }
    ASSERT_TRUE(sampler2->getNext(/*batchSize*/ 3).empty());

    ASSERT_TRUE(sampler3->doneSampling());
    i = 10;
    while (i < 15) {
        auto docs = sampler3->getNext(/*batchSize*/ 1);
        ASSERT_TRUE(docs.size() <= 1);
        for (auto& doc : docs) {
            ASSERT_BSONOBJ_EQ(inputDocs[i], doc);
            ++i;
        }
    }
    ASSERT_TRUE(sampler3->getNext(/*batchSize*/ 1).empty());
}

}  // namespace streams
