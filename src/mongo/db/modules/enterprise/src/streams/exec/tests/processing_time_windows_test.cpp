/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/tick_source_mock.h"
#include "streams/exec/context.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include "streams/exec/tests/test_utils.h"

namespace streams {

using namespace mongo;

class ProcessingTimeWindowsTest : public AggregationContextFixture {
public:
    ProcessingTimeWindowsTest() {
        _context = get<0>(getTestContext(nullptr));
    }
    std::vector<BSONObj> parsePipeline(const std::string& pipeline) {
        const auto inputBson = fromjson("{pipeline: " + pipeline + "}");
        ASSERT_EQ(inputBson["pipeline"].type(), BSONType::Array);
        return parsePipelineFromBSON(inputBson["pipeline"]);
    }

    void setupConnections() {
        Connection atlasConn{};
        atlasConn.setName("atlas");
        AtlasConnectionOptions atlasConnOptions{"mongodb://localhost:270"};
        atlasConn.setOptions(atlasConnOptions.toBSON());
        atlasConn.setType(ConnectionTypeEnum::Atlas);

        _context->connections = {
            {atlasConn.getName().toString(), atlasConn},
        };
    }

    std::string makePipelineStr(std::string windowStr) {
        return _sourceStr + windowStr + _mergeStr;
    }

    void setupFFTest() {
        setupConnections();
        mongo::stdx::unordered_map<std::string, mongo::Value> featureFlagsMap;
        featureFlagsMap[FeatureFlags::kProcessingTimeWindows.name] = mongo::Value(true);
        StreamProcessorFeatureFlags spFeatureFlags{
            featureFlagsMap,
            std::chrono::time_point<std::chrono::system_clock>{
                std::chrono::system_clock::now().time_since_epoch()}};
        _context->featureFlags->updateFeatureFlags(spFeatureFlags);
    }

protected:
    std::unique_ptr<Context> _context;
    std::string _sourceStr = R"([
{
    $source: {
        connectionName: "atlas"
    }
},)";
    std::string _mergeStr = R"(
    {
    $merge: {
        into: {
            connectionName: "atlas",
            db: "test1",
            coll: "test1"
        }
    }
}
])";
};

namespace {

TEST_F(ProcessingTimeWindowsTest, TumblingWindowIncompatibleOptionsAllowedLateness) {
    setupFFTest();
    Planner planner(_context.get(), Planner::Options());
    std::string tumblingWindowStr = R"(
{
$tumblingWindow: {
    boundary: "processingTime",
    interval: {size: 5, unit: "second"},
    allowedLateness: {size: 5, unit: "second"},
    pipeline: []
}
},
    )";
    auto bson = parsePipeline(makePipelineStr(tumblingWindowStr));
    const auto expectedWhat =
        "StreamProcessorInvalidOptions: Cannot specify allowed lateness value for a processing "
        "time window";

    ASSERT_THROWS_CODE_AND_WHAT(planner.plan(bson),
                                AssertionException,
                                ErrorCodes::StreamProcessorInvalidOptions,
                                expectedWhat);
}

TEST_F(ProcessingTimeWindowsTest, TumblingWindowIncompatibleOptionsIdleTimeout) {
    setupFFTest();
    Planner planner(_context.get(), Planner::Options());
    std::string tumblingWindowStr = R"(
{
$tumblingWindow: {
    boundary: "processingTime",
    interval: {size: 5, unit: "second"},
    idleTimeout: {size: 5, unit: "second"},
    pipeline: []
}
},
    )";
    auto bson = parsePipeline(makePipelineStr(tumblingWindowStr));
    const auto expectedWhat =
        "StreamProcessorInvalidOptions: Cannot specify idle timeout value for a processing time "
        "window";

    ASSERT_THROWS_CODE_AND_WHAT(planner.plan(bson),
                                AssertionException,
                                ErrorCodes::StreamProcessorInvalidOptions,
                                expectedWhat);
}

TEST_F(ProcessingTimeWindowsTest, HoppingWindowIncompatibleOptionsAllowedLateness) {
    setupFFTest();
    Planner planner(_context.get(), Planner::Options());
    std::string hoppingWindowStr = R"(
{
$hoppingWindow: {
    boundary: "processingTime",
    interval: {size: 5, unit: "second"},
    hopSize: {size: 1, unit: "second"},
    allowedLateness: {size: 5, unit: "second"},
    pipeline: []
}
},
    )";
    auto bson = parsePipeline(makePipelineStr(hoppingWindowStr));
    const auto expectedWhat =
        "StreamProcessorInvalidOptions: Cannot specify allowed lateness value for a processing "
        "time window";

    ASSERT_THROWS_CODE_AND_WHAT(planner.plan(bson),
                                AssertionException,
                                ErrorCodes::StreamProcessorInvalidOptions,
                                expectedWhat);
}

TEST_F(ProcessingTimeWindowsTest, HoppingWindowIncompatibleOptionsIdleTimeout) {
    setupFFTest();
    Planner planner(_context.get(), Planner::Options());
    std::string hoppingWindowStr = R"(
{
$hoppingWindow: {
    boundary: "processingTime",
    interval: {size: 5, unit: "second"},
    hopSize: {size: 1, unit: "second"},
    idleTimeout: {size: 5, unit: "second"},
    pipeline: []
}
},
    )";
    auto bson = parsePipeline(makePipelineStr(hoppingWindowStr));
    const auto expectedWhat =
        "StreamProcessorInvalidOptions: Cannot specify idle timeout value for a processing time "
        "window";

    ASSERT_THROWS_CODE_AND_WHAT(planner.plan(bson),
                                AssertionException,
                                ErrorCodes::StreamProcessorInvalidOptions,
                                expectedWhat);
}

TEST_F(ProcessingTimeWindowsTest, FeatureFlagDisabled) {
    setupConnections();
    Planner planner(_context.get(), Planner::Options());
    std::string hoppingWindowStr = R"(
{
$hoppingWindow: {
    boundary: "processingTime",
    interval: {size: 5, unit: "second"},
    hopSize: {size: 1, unit: "second"},
    idleTimeout: {size: 5, unit: "second"},
    pipeline: []
}
},
    )";
    auto bson = parsePipeline(makePipelineStr(hoppingWindowStr));

    const auto expectedWhat =
        "StreamProcessorInvalidOptions: Processing time windows are not supported";

    ASSERT_THROWS_CODE_AND_WHAT(planner.plan(bson),
                                AssertionException,
                                ErrorCodes::StreamProcessorInvalidOptions,
                                expectedWhat);
}

TEST_F(ProcessingTimeWindowsTest, WindowsClosingAsExpected) {
    auto innerTest = [&](std::string windowPipeline) {
        setupFFTest();
        auto prevConnections = _context->connections;
        _context->connections = testInMemoryConnectionRegistry();
        ScopeGuard guard([&] { _context->connections = prevConnections; });
        Planner planner(_context.get(), {});
        std::string pipeline = R"(
    [
        { $source: { connectionName: "__testMemory" }},
    {)" + windowPipeline +
            R"({ $emit: {connectionName: "__testMemory"}}
    ]
        )";

        auto dag = planner.plan(
            parsePipelineFromBSON(fromjson("{pipeline: " + pipeline + "}")["pipeline"]));
        dag->start();

        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 3);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "SortOperator");
        ASSERT_EQ(ops[2]->getName(), "InMemorySinkOperator");

        auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
        TickSourceMock tickSource{};
        Timer timer{&tickSource};
        source->setMockTimer(&timer);
        auto sink = dynamic_cast<InMemorySinkOperator*>(dag->operators().back().get());

        int64_t startTimeFirstWindow = timer.millis();
        int64_t curTimeFirstWindow = timer.millis();
        for (int i = 0; i < 5; i++) {
            auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
            dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}, b: {}}}", i, i))));
            source->addDataMsg(dataMsg);
            source->runOnce();
            curTimeFirstWindow = timer.millis();
            tickSource.advance(Seconds(1));
        }
        int64_t endTimeFirstWindow = timer.millis();

        int64_t startTimeSecondWindow = timer.millis();
        int64_t curTimeSecondWindow = timer.millis();
        for (int i = 0; i < 5; i++) {
            auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
            dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: {}, b: {}}}", i, i))));
            source->addDataMsg(dataMsg);
            source->runOnce();
            curTimeSecondWindow = timer.millis();
            tickSource.advance(Seconds(1));
        }
        int64_t endTimeSecondWindow = timer.millis();

        auto messages = sink->getMessages();
        size_t messagesSize = messages.size();

        // One message should be control message, the other should be a data message.
        ASSERT_EQ(messagesSize, 2);
        for (size_t i = 0; i < messagesSize; i++) {
            StreamMsgUnion msg = std::move(messages.front());
            messages.pop_front();
            if (msg.controlMsg) {
                continue;
            }
            ASSERT_EQ(msg.dataMsg->docs.size(), 5);
            for (size_t i = 0; i < msg.dataMsg->docs.size(); i++) {
                StreamDocument doc = msg.dataMsg->docs[i];
                ASSERT_EQ(doc.minDocTimestampMs, startTimeFirstWindow);
                ASSERT_EQ(doc.maxDocTimestampMs, curTimeFirstWindow);
                ASSERT_EQ(doc.streamMeta.getWindow()->getStart()->toMillisSinceEpoch(),
                          startTimeFirstWindow);
                ASSERT_EQ(doc.streamMeta.getWindow()->getEnd()->toMillisSinceEpoch(),
                          endTimeFirstWindow);
            }
        }

        // Add one more message to close the second window
        tickSource.advance(Seconds(6));
        auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{a: 6, b: 6}}"))));
        source->addDataMsg(dataMsg);
        source->runOnce();

        messages = sink->getMessages();
        messagesSize = messages.size();
        // One message should be control message, the other should be a data message.
        ASSERT_EQ(messagesSize, 2);
        for (size_t i = 0; i < messagesSize; i++) {
            StreamMsgUnion msg = std::move(messages.front());
            messages.pop_front();
            if (msg.controlMsg) {
                continue;
            }
            ASSERT_EQ(msg.dataMsg->docs.size(), 5);
            for (size_t i = 0; i < msg.dataMsg->docs.size(); i++) {
                StreamDocument doc = msg.dataMsg->docs[i];
                ASSERT_EQ(doc.minDocTimestampMs, startTimeSecondWindow);
                ASSERT_EQ(doc.maxDocTimestampMs, curTimeSecondWindow);
                ASSERT_EQ(doc.streamMeta.getWindow()->getStart()->toMillisSinceEpoch(),
                          startTimeSecondWindow);
                ASSERT_EQ(doc.streamMeta.getWindow()->getEnd()->toMillisSinceEpoch(),
                          endTimeSecondWindow);
            }
        }
    };

    // This tests processing time boundaries for $tumblingWindow
    innerTest(R"($tumblingWindow: {
        boundary: "processingTime",
        interval: {size: 5, unit: "second"},
        pipeline: [
        {$sort: {a: 1}}
        ]
    }
    },)");

    // This tests processing time boundaries for $hoppingWindow
    innerTest(R"($hoppingWindow: {
    boundary: "processingTime",
    hopSize: {size: 5, unit: "second"},
    interval: {size: 5, unit: "second"},
    pipeline: [
    {$sort: {a: 1}}
    ]
}
},)");
}
}  // namespace
}  // namespace streams
