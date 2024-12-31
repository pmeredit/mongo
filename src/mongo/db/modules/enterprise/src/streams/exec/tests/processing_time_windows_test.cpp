/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
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
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
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
}  // namespace
}  // namespace streams
