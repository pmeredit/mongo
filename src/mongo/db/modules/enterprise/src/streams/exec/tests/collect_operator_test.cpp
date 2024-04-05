/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/collect_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

class CollectOperatorTest : public AggregationContextFixture {
public:
    CollectOperatorTest()
        : _metricManager(std::make_unique<MetricManager>()),
          _context(std::get<0>(getTestContext(nullptr /* svcCtx */))) {}

    std::unique_ptr<CollectOperator> makeOperator() const {
        return std::make_unique<CollectOperator>(_context.get(), 1 /* numInputs */);
    }

    static StreamDataMsg makeDataMessage(std::vector<BSONObj> documents) {
        std::vector<StreamDocument> out;
        out.reserve(documents.size());
        std::transform(
            documents.begin(), documents.end(), std::back_inserter(out), [&](BSONObj& document) {
                return StreamDocument(Document(std::move(document)));
            });
        return StreamDataMsg{out};
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};  // class CollectOperatorTest

TEST_F(CollectOperatorTest, AddAndGet) {
    auto collectOperator = makeOperator();
    collectOperator->start();
    ASSERT_EQUALS(0, collectOperator->getStats().memoryUsageBytes);

    collectOperator->onDataMsg(0 /* inputIdx */,
                               makeDataMessage({
                                   BSON("id" << 1),
                                   BSON("id" << 2),
                               }));
    ASSERT_EQUALS(250, collectOperator->getStats().memoryUsageBytes);

    // Add one more message which should increase the memory usage.
    collectOperator->onDataMsg(0 /* inputIdx */,
                               makeDataMessage({
                                   BSON("id" << 3),
                               }));
    ASSERT_EQUALS(375, collectOperator->getStats().memoryUsageBytes);

    auto msgs = collectOperator->getMessages();
    ASSERT_EQUALS(2, msgs.size());
    ASSERT_DOCUMENT_EQ(Document(BSON("id" << 1)), msgs[0].dataMsg->docs[0].doc);
    ASSERT_DOCUMENT_EQ(Document(BSON("id" << 2)), msgs[0].dataMsg->docs[1].doc);
    ASSERT_DOCUMENT_EQ(Document(BSON("id" << 3)), msgs[1].dataMsg->docs[0].doc);

    // Fetching all the messages from the collect operator should clear out its slice
    // of messages and transfer ownership of the messages to the caller of `getMessages()`.
    ASSERT_EQUALS(0, collectOperator->getStats().memoryUsageBytes);
    msgs = collectOperator->getMessages();
    ASSERT_TRUE(msgs.empty());
}

}  // namespace streams
