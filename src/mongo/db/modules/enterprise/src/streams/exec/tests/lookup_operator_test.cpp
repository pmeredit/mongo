/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/lookup_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/planner.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;

class LookUpOperatorTest : public AggregationContextFixture {
public:
    LookUpOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
        _context = std::get<0>(getTestContext(getServiceContext()));
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

// Executes $lookup using a collection in local MongoDB deployment.
TEST_F(LookUpOperatorTest, LocalTest) {
    // Sample value for the envvar: mongodb://localhost:27017
    if (!std::getenv("LOOKUP_TEST_MONGODB_URI")) {
        std::cerr << "Warning: Skipping test since LOOKUP_TEST_MONGODB_URI is not defined"
                  << std::endl;
        return;
    }

    Connection atlasConn;
    atlasConn.setName("myconnection");
    AtlasConnectionOptions atlasConnOptions{std::getenv("LOOKUP_TEST_MONGODB_URI")};
    atlasConn.setOptions(atlasConnOptions.toBSON());
    atlasConn.setType(ConnectionTypeEnum::Atlas);
    _context->connections = testInMemoryConnectionRegistry();
    _context->connections.insert(std::make_pair(atlasConn.getName().toString(), atlasConn));

    NamespaceString fromNs =
        NamespaceString::createNamespaceString_forTest(boost::none, "test", "foreign_coll");
    _context->expCtx->setResolvedNamespaces(
        StringMap<ResolvedNamespace>{{fromNs.coll().toString(), {fromNs, std::vector<BSONObj>()}}});

    auto lookupObj = fromjson(R"(
{
  $lookup: {
    from: {
      connectionName: "myconnection",
      db: "test",
      coll: "foreign_coll"
    },
    localField: "leftKey",
    foreignField: "rightKey",
    as: "arr"
  }
})");
    auto unwindObj = fromjson(R"(
{
  $unwind: {
    path: "$arr"
  }
})");

    std::vector<BSONObj> rawPipeline{
        getTestSourceSpec(), lookupObj, unwindObj, getTestMemorySinkSpec()};

    Planner planner(_context.get(), /*options*/ {});
    auto dag = planner.plan(rawPipeline);
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    std::vector<BSONObj> inputDocs;
    std::vector<int> vals = {5, 2};
    inputDocs.reserve(vals.size());
    for (auto& val : vals) {
        inputDocs.emplace_back(fromjson(fmt::format("{{leftKey: {}}}", val)));
    }

    StreamDataMsg dataMsg;
    for (auto& inputDoc : inputDocs) {
        dataMsg.docs.emplace_back(Document(inputDoc));
    }
    source->addDataMsg(std::move(dataMsg));
    source->runOnce();

    std::deque<StreamMsgUnion> msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    auto msgUnion = std::move(msgs.front());
    msgs.pop_front();
    ASSERT_TRUE(msgUnion.dataMsg);
    ASSERT_EQUALS(3, msgUnion.dataMsg->docs.size());
}

// Executes a collectionless $lookup with $documents
TEST_F(LookUpOperatorTest, LocalTestCollectionlessLookupWithDocuments) {
    _context->connections = testInMemoryConnectionRegistry();
    auto lookupObj = fromjson(R"(
{
  $lookup: {
    localField: "leftKey",
    foreignField: "rightKey",
    as: "arr",
    pipeline: [{
        $documents: [{
            rightKey: 2,
            a: 1
        }, {
            rightKey: 2,
            a: 2
        }, {
            rightKey: 5,
            a: 1
        }, {
            rightKey: 1,
            a: 1
        }]
    }]
  }
})");
    auto unwindObj = fromjson(R"(
{
  $unwind: {
    path: "$arr"
  }
})");

    std::vector<BSONObj> rawPipeline{
        getTestSourceSpec(), lookupObj, unwindObj, getTestMemorySinkSpec()};

    Planner planner(_context.get(), /*options*/ {});
    auto dag = planner.plan(rawPipeline);
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

    std::vector<BSONObj> inputDocs;
    std::vector<int> vals = {5, 2};
    inputDocs.reserve(vals.size());
    for (auto& val : vals) {
        inputDocs.emplace_back(fromjson(fmt::format("{{leftKey: {}}}", val)));
    }

    StreamDataMsg dataMsg;
    for (auto& inputDoc : inputDocs) {
        dataMsg.docs.emplace_back(Document(inputDoc));
    }
    source->addDataMsg(std::move(dataMsg));
    source->runOnce();

    std::deque<StreamMsgUnion> msgs = sink->getMessages();
    ASSERT_EQUALS(1, msgs.size());
    auto msgUnion = std::move(msgs.front());
    msgs.pop_front();
    ASSERT_TRUE(msgUnion.dataMsg);
    ASSERT_EQUALS(3, msgUnion.dataMsg->docs.size());
}

}  // namespace
}  // namespace streams
