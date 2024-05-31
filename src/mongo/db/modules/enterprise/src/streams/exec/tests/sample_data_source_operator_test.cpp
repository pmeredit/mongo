/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "mongo/unittest/assert.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/sample_data_source_operator.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

using SampleDataSourceOperatorTest = AggregationContextFixture;

namespace {

TEST_F(SampleDataSourceOperatorTest, Basic) {
    // Create a test context.
    auto [context, _] = getTestContext(/*svcCtx*/ nullptr);

    // Create the sample data $source and in-memory sink.
    SampleDataSourceOperator::Options options;
    // Use a seed so we get the same data.
    options.seed = 42;
    SampleDataSourceOperator source{context.get(), std::move(options)};
    InMemorySinkOperator sink{context.get(), 1 /* numInputs */};
    source.addOutput(&sink, 0 /* outputOperatorIdx */);

    // Test the sample data $source to run once.
    source.runOnce();

    // Verify the output messages in the sink.
    auto messages = sink.getMessages();
    ASSERT_EQ(1, messages.size());
    ASSERT(messages[0].dataMsg);
    const auto& docs = messages[0].dataMsg->docs;
    ASSERT_EQ(2, docs.size());
    for (const auto& doc : docs) {
        // Verify the output docs are of the expected schema.
        auto data =
            SampleDataSourceSolarSpec::parseOwned(IDLParserContext{"test"}, doc.doc.toBson());
        int32_t watts = data.getMax_watts();
        ASSERT_GTE(watts, 0 /* minWatts in sample_data_source_operator.cpp */);
        ASSERT_LTE(watts, 250 /* maxWatts in sample_data_source_operator.cpp */);
    }
}

}  // namespace
}  // namespace streams
