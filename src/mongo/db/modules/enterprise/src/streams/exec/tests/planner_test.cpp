/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <algorithm>
#include <chrono>
#include <fmt/format.h>
#include <iostream>
#include <rdkafkacpp.h>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/pipeline/document_source_change_stream_gen.h"
#include "mongo/db/query/sbe_stage_builder_helpers.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/s/sharding_state.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/add_fields_operator.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_emit_operator.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/noop_sink_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/planner.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
#include "streams/exec/window_aware_limit_operator.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

class PlannerTest : public AggregationContextFixture {
public:
    PlannerTest() {
        ShardingState::create(getServiceContext());
        _metricManager = std::make_unique<MetricManager>();
        _context = get<0>(getTestContext(nullptr));
    }

    std::unique_ptr<OperatorDag> addSourceSinkAndParse(std::vector<BSONObj> rawPipeline) {
        _context->connections = testInMemoryConnectionRegistry();
        Planner planner(_context.get(), /*options*/ {});
        if (rawPipeline.size() == 0 ||
            rawPipeline.front().firstElementFieldName() != std::string{"$source"}) {
            rawPipeline.insert(rawPipeline.begin(), getTestSourceSpec());
        }

        if (rawPipeline.back().firstElementFieldName() != std::string{"$emit"}) {
            rawPipeline.push_back(getTestLogSinkSpec());
        }

        return planner.plan(rawPipeline);
    }

    std::unique_ptr<OperatorDag> addSourceSinkAndParse(const std::string& pipeline) {
        return addSourceSinkAndParse(parsePipeline(pipeline));
    }

    std::vector<BSONObj> parsePipeline(const std::string& pipeline) {
        const auto inputBson = fromjson("{pipeline: " + pipeline + "}");
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
        return parsePipelineFromBSON(inputBson["pipeline"]);
    }

    BSONObj addFieldsStage(int i) {
        return BSON("$addFields" << BSON(std::to_string(i) << i));
    }

    BSONObj sourceStage() {
        return getTestSourceSpec();
    }

    BSONObj emitStage() {
        return getTestLogSinkSpec();
    }

    BSONObj groupStage() {
        return BSON("$group" << BSON("_id" << BSONNULL << "sum"
                                           << BSON("$sum"
                                                   << "$field")));
    }

    BSONObj sortStage() {
        return BSON("$sort" << BSON("_id" << 1));
    }

    BSONObj limitStage() {
        return BSON("$limit" << 10);
    }

    KafkaConsumerOperator::ConsumerInfo& getConsumerInfo(
        KafkaConsumerOperator* kafkaConsumerOperator, size_t idx) {
        return kafkaConsumerOperator->_consumers[idx];
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
};

namespace {

TEST_F(PlannerTest, RegularParsingErrorsWork) {
    std::vector<BSONObj> invalidBsonPipeline{
        BSON("$addFields" << 1),
    };
    ASSERT_THROWS_CODE(addSourceSinkAndParse(invalidBsonPipeline), AssertionException, 40272);
}

TEST_F(PlannerTest, OnlySupportedStages) {
    std::string pipeline = R"(
[
    { $match: { a: 1 }},
    { $project: { a: 1 }},
    { $densify: { field: "timestamp", range: { step: 1, unit: "hour" } } }
]
    )";

    // We don't support $densify.
    ASSERT_THROWS_CODE(
        addSourceSinkAndParse(pipeline), AssertionException, ErrorCodes::InvalidOptions);
}


/**
Parse a user defined pipeline with all the supported MDP mapping stages.
Verify that we can create an OperatorDag from it, and that the operators
are of the correct type.
*/
TEST_F(PlannerTest, SupportedStagesWork1) {
    std::string pipeline = R"(
[
    { $addFields: { a: 1 } },
    { $match: { a: 1 } },
    { $project: { a: 1 } },
    { $redact: { $cond: {
        if: { $eq: [ "$a", 1 ] },
        then: "$$DESCEND",
        else: "$$PRUNE"
    }}},
    { $replaceRoot: { newRoot: "$name" }},
    { $replaceWith: "$name" },
    { $set: {
        b: 1
    }},
    { $unset: "copies" },
    { $unwind: "$sizes" }
]
    )";

    std::unique_ptr<OperatorDag> dag(addSourceSinkAndParse(pipeline));
    auto& ops = dag->operators();
    ASSERT_EQ(ops.size(), 9 /* pipeline stages */ + 2 /* Source and Sink */);
    ASSERT_EQ(ops[1]->getName(), "AddFieldsOperator");
    ASSERT_EQ(ops[2]->getName(), "MatchOperator");
    ASSERT_EQ(ops[3]->getName(), "ProjectOperator");
    ASSERT_EQ(ops[4]->getName(), "RedactOperator");
    ASSERT_EQ(ops[5]->getName(), "ReplaceRootOperator");
    ASSERT_EQ(ops[6]->getName(), "ReplaceRootOperator");  // From the user's $replaceWith.
    ASSERT_EQ(ops[7]->getName(), "SetOperator");
    ASSERT_EQ(ops[8]->getName(), "ProjectOperator");  // From the user's $unset.
    ASSERT_EQ(ops[9]->getName(), "UnwindOperator");
}

/**
Put together different permutations of valid pipelines, and make sure they can
be parsed into an OpereatorDag.
We don't do much validation here on the results here,
other than "at least one operator was created" and "parsing didn't crash".
*/
TEST_F(PlannerTest, SupportedStagesWork2) {
    std::vector<BSONObj> validStages{
        BSON("$addFields" << BSON("a" << 1)),
        BSON("$match" << BSON("a" << 1)),
        BSON("$project" << BSON("a" << 1)),
        BSON(
            "$redact" << BSON("$cond" << BSON("if" << BSON("$eq" << BSON_ARRAY("$a" << 1)) << "then"
                                                   << "$$DESCEND"
                                                   << "else"
                                                   << "$$PRUNE"))),
        BSON("$replaceRoot" << BSON("newRoot"
                                    << "$name")),
        BSON("$set" << BSON("b" << 1)),
        BSON("$unwind"
             << "$sizes"),
        BSON("$validate" << BSON("validator"
                                 << BSON("$jsonSchema" << BSON("required" << BSON_ARRAY("a"))))),
    };

    std::vector<std::vector<BSONObj>> validBsonPipelines;
    for (size_t i = 0; i < validStages.size(); i++) {
        for (size_t j = 0; j < validStages.size(); j++) {
            for (size_t k = 0; k < validStages.size(); k++) {
                std::vector<BSONObj> pipeline{validStages[i], validStages[j], validStages[k]};
                validBsonPipelines.push_back(pipeline);
            }
        }
    }

    for (const auto& pipeline : validBsonPipelines) {
        std::unique_ptr<OperatorDag> dag(addSourceSinkAndParse(pipeline));
        const auto& ops = dag->operators();
        ASSERT_GTE(ops.size(), 1);
    }
}

TEST_F(PlannerTest, MergeStageParsing) {
    Connection atlasConn;
    atlasConn.setName("myconnection");
    AtlasConnectionOptions atlasConnOptions{"mongodb://localhost:270"};
    atlasConn.setOptions(atlasConnOptions.toBSON());
    atlasConn.setType(ConnectionTypeEnum::Atlas);
    _context->connections =
        stdx::unordered_map<std::string, Connection>{{atlasConn.getName().toString(), atlasConn}};
    auto inMemoryConnection = testInMemoryConnectionRegistry();
    _context->connections.insert(inMemoryConnection.begin(), inMemoryConnection.end());

    std::vector<BSONObj> rawPipeline{
        getTestSourceSpec(), fromjson("{ $addFields: { a: 5 } }"), fromjson(R"(
{
  $merge: {
    into: {
      connectionName: "myconnection",
      db: "mydb",
      coll: "mycoll"
    },
    whenMatched: "replace",
    whenNotMatched: "insert"
  }
})")};

    Planner planner(_context.get(), /*options*/ {});
    auto dag = planner.plan(rawPipeline);
    const auto& ops = dag->operators();
    ASSERT_GTE(ops.size(), 1);
}

TEST_F(PlannerTest, LookUpStageParsing) {
    Connection atlasConn;
    atlasConn.setName("myconnection");
    AtlasConnectionOptions atlasConnOptions{"mongodb://localhost:270"};
    atlasConn.setOptions(atlasConnOptions.toBSON());
    atlasConn.setType(ConnectionTypeEnum::Atlas);
    _context->connections =
        stdx::unordered_map<std::string, Connection>{{atlasConn.getName().toString(), atlasConn}};
    auto inMemoryConnection = testInMemoryConnectionRegistry();
    _context->connections.insert(inMemoryConnection.begin(), inMemoryConnection.end());

    auto addFieldsObj = fromjson("{ $addFields: { leftKey: 5 } }");
    auto lookupObj = fromjson(R"(
{
  $lookup: {
    from: {
      connectionName: "myconnection",
      db: "test",
      coll: "input_coll"
    },
    localField: "leftKey",
    foreignField: "rightKey",
    as: "arr"
  }
})");

    {
        std::vector<BSONObj> rawPipeline{
            getTestSourceSpec(), addFieldsObj, lookupObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 4);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "AddFieldsOperator");
        ASSERT_EQ(ops[2]->getName(), "LookUpOperator");
        ASSERT_EQ(ops[3]->getName(), "LogSinkOperator");
    }

    {
        auto unwindObj = fromjson(R"(
{
  $unwind: {
    path: "$arr"
  }
})");
        std::vector<BSONObj> rawPipeline{
            getTestSourceSpec(), addFieldsObj, lookupObj, unwindObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        // Verify that $unwind got merged into $lookup, so there are still 4 operators in the dag.
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 4);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "AddFieldsOperator");
        ASSERT_EQ(ops[2]->getName(), "LookUpOperator");
        ASSERT_EQ(ops[3]->getName(), "LogSinkOperator");
    }

    {
        auto unwindObj = fromjson(R"(
{
  $unwind: {
    path: "$arr"
  }
})");
        auto matchObj = fromjson(R"(
{
  $match: {
    "arr.a": 2
  }
})");
        std::vector<BSONObj> rawPipeline{getTestSourceSpec(),
                                         addFieldsObj,
                                         lookupObj,
                                         unwindObj,
                                         matchObj,
                                         getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        // Verify that both $unwind and $match got merged into $lookup, so there are still 4
        // operators in the dag.
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 4);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "AddFieldsOperator");
        ASSERT_EQ(ops[2]->getName(), "LookUpOperator");
        ASSERT_EQ(ops[3]->getName(), "LogSinkOperator");
    }

    {
        auto windowObj = fromjson(R"(
{
    $tumblingWindow: {
        interval: {size: 5, unit: "second"},
        pipeline: [
            { $group: { _id : null, sum: {$sum: "$a"} }},
            { $sort: { "sum" : 1 }},
            { $limit: 5 },
            {
              $lookup: {
                from: {
                  connectionName: "myconnection",
                  db: "test",
                  coll: "input_coll"
                },
                localField: "sum",
                foreignField: "sum",
                as: "arr"
              }
            }
        ]
    }
})");
        std::vector<BSONObj> rawPipeline{
            getTestSourceSpec(), addFieldsObj, windowObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 6);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "AddFieldsOperator");
        ASSERT_EQ(ops[2]->getName(), "GroupOperator");
        ASSERT_EQ(ops[3]->getName(), "SortOperator");
        ASSERT_EQ(ops[4]->getName(), "LookUpOperator");
        ASSERT_EQ(ops[5]->getName(), "LogSinkOperator");
    }

    {
        auto lookupObj = fromjson(R"(
{
  $lookup: {
    from: {
      connectionName: "myconnection",
      db: "test",
      coll: "input_coll"
    },
    let: { productIds: "$productIds" },
    pipeline: [
      {
        $match: {
          $expr: { $in: ["$_id", "$$productIds"] }
        }
      }
    ],
    as: "arr"
  }
})");
        std::vector<BSONObj> rawPipeline{
            getTestSourceSpec(), addFieldsObj, lookupObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 4);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "AddFieldsOperator");
        ASSERT_EQ(ops[2]->getName(), "LookUpOperator");
        ASSERT_EQ(ops[3]->getName(), "LogSinkOperator");
    }
}

TEST_F(PlannerTest, WindowStageParsing) {
    _context->connections = testInMemoryConnectionRegistry();

    {
        auto windowObj = fromjson(R"(
{
    $tumblingWindow: {
        interval: {size: 5, unit: "second"},
        pipeline: [
        ]
    }
})");
        std::vector<BSONObj> rawPipeline{getTestSourceSpec(), windowObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 3);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "LimitOperator");
        ASSERT(dynamic_cast<WindowAwareLimitOperator*>(ops[1].get()));
        ASSERT_EQ(ops[2]->getName(), "LogSinkOperator");
    }

    {
        auto windowObj = fromjson(R"(
{
    $hoppingWindow: {
        interval: {size: 5, unit: "second"},
        hopSize: {size: 1, unit: "second"},
        pipeline: [
            { $count: "a" }
        ]
    }
})");
        std::vector<BSONObj> rawPipeline{getTestSourceSpec(), windowObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 4);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "GroupOperator");
        ASSERT_EQ(ops[2]->getName(), "ProjectOperator");
        ASSERT_EQ(ops[3]->getName(), "LogSinkOperator");
    }

    {
        auto windowObj = fromjson(R"(
{
    $tumblingWindow: {
        interval: {size: 5, unit: "second"},
        pipeline: [
            { $source: { connectionName: "__testMemory" }}
        ]
    }
})");
        std::vector<BSONObj> rawPipeline{getTestSourceSpec(), windowObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        ASSERT_THROWS_CODE_AND_WHAT(planner.plan(rawPipeline),
                                    DBException,
                                    ErrorCodes::InvalidOptions,
                                    "Unsupported stage: $source");
    }

    {
        auto windowObj = fromjson(R"(
{
    $hoppingWindow: {
        interval: {size: 5, unit: "second"},
        hopSize: {size: 1, unit: "second"},
        pipeline: [
            { $source: { connectionName: "__testMemory" }}
        ]
    }
})");
        std::vector<BSONObj> rawPipeline{getTestSourceSpec(), windowObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), /*options*/ {});
        ASSERT_THROWS_CODE_AND_WHAT(planner.plan(rawPipeline),
                                    DBException,
                                    ErrorCodes::InvalidOptions,
                                    "Unsupported stage: $source");
    }
}

TEST_F(PlannerTest, WindowStageParsingUnnested) {
    _context->connections = testInMemoryConnectionRegistry();

    {
        auto windowObj = fromjson(R"(
{
    $hoppingWindow: {
        interval: {size: 5, unit: "second"},
        hopSize: {size: 1, unit: "second"},
        pipeline: [
            { $count: "a" }
        ]
    }
})");
        std::vector<BSONObj> rawPipeline{getTestSourceSpec(), windowObj, getTestLogSinkSpec()};

        Planner planner(_context.get(), Planner::Options{});
        auto dag = planner.plan(rawPipeline);
        const auto& ops = dag->operators();
        ASSERT_EQ(ops.size(), 4);
        ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
        ASSERT_EQ(ops[1]->getName(), "GroupOperator");
        ASSERT_EQ(ops[2]->getName(), "ProjectOperator");
        ASSERT_EQ(ops[3]->getName(), "LogSinkOperator");
    }

    // TODO: add more tests from above
}

/**
 * Verify that we're taking advantage of the pipeline->optimize logic.
 * The two $match stages should be merged into one.
 */
TEST_F(PlannerTest, StagesOptimized) {
    std::vector<BSONObj> pipeline{BSON("$addFields" << BSON("a" << 1)),
                                  BSON("$match" << BSON("a" << 1)),
                                  BSON("$match" << BSON("a" << 1))};

    std::unique_ptr<OperatorDag> dag = addSourceSinkAndParse(pipeline);
    const auto& ops = dag->operators();
    ASSERT_EQ(ops.size(), 4);
    ASSERT_EQ(ops[0]->getName(), "InMemorySourceOperator");
    ASSERT_EQ(ops[1]->getName(), "AddFieldsOperator");
    ASSERT_EQ(ops[2]->getName(), "MatchOperator");
    ASSERT_EQ(ops[3]->getName(), "LogSinkOperator");
}

TEST_F(PlannerTest, InvalidPipelines) {
    auto validStage = [](int i) {
        return BSONObj{BSON("$addFields" << BSON(std::to_string(i) << i))};
    };

    std::vector<std::vector<BSONObj>> pipelines{
        std::vector<BSONObj>{},
        std::vector<BSONObj>{validStage(0)},
        std::vector<BSONObj>{sourceStage(), validStage(0)},
        std::vector<BSONObj>{validStage(0), emitStage()},
        std::vector<BSONObj>{validStage(0), sourceStage(), emitStage()},
        std::vector<BSONObj>{emitStage(), validStage(0), sourceStage()}};
    for (const auto& pipeline : pipelines) {
        Planner planner(_context.get(), /*options*/ {});
        ASSERT_THROWS_CODE(planner.plan(pipeline), DBException, ErrorCodes::InvalidOptions);
    }
}

/**
 * Verify that the operators in the parsed OperatorDag are in the correct order, according to the
 * user pipeline.
 */
TEST_F(PlannerTest, OperatorOrder) {
    std::vector<int> numStages{0, 2, 10, 100};
    const std::string field{"a"};
    for (int numStage : numStages) {
        std::vector<BSONObj> pipeline;
        for (int i = 0; i < numStage; i++) {
            pipeline.push_back(BSON("$addFields" << BSON(field << i)));
        }
        std::unique_ptr<OperatorDag> dag = addSourceSinkAndParse(pipeline);
        ASSERT_EQ(dag->operators().size(), numStage + 2);
        for (int i = 0; i < numStage; i += 1) {
            auto& op = dag->operators()[i + 1];
            ASSERT_EQ(op->getName(), "AddFieldsOperator");
            auto addFieldsOp = dynamic_cast<AddFieldsOperator*>(op.get());

            std::vector<Value> ser;
            addFieldsOp->documentSource()->serializeToArray(ser);
            ASSERT_EQ(ser.size(), 1);
            const Value actual = ser[0]["$addFields"][field]["$const"];
            ASSERT_EQ(actual.getInt(), i);
        }
    }
}

/**
 * Verifies we can parse the Kafka source spec
 * See stages.idl
        { $source: {
            connectionName: string,
            topic: string
            timeField: optional<object>,
            tsFieldOverride: optional<string>,
            testOnlyPartitionCount: optional<int>,
        }},
 */
TEST_F(PlannerTest, KafkaSourceParsing) {
    static std::string streamProcessorId = "sp1";

    Connection kafka1;
    kafka1.setName("myconnection");
    KafkaConnectionOptions options1{"localhost:9092"};
    options1.setIsTestKafka(true);
    kafka1.setOptions(options1.toBSON());
    kafka1.setType(ConnectionTypeEnum::Kafka);

    Connection kafka2;
    kafka2.setName("myconnection2");
    KafkaConnectionOptions options2{"localhost:9093"};
    options2.setIsTestKafka(true);
    options2.setAuth(KafkaAuthOptions::parse(IDLParserContext("KafkaAuthOptions"), fromjson(R"({
        "saslUsername": "user123",
        "saslPassword": "foo12345",
        "saslMechanism": "PLAIN",
        "securityProtocol": "SASL_PLAINTEXT"
    })")));
    kafka2.setOptions(options2.toBSON());
    kafka2.setType(ConnectionTypeEnum::Kafka);

    Connection kafka3;
    kafka3.setName("kafka3");
    KafkaConnectionOptions options3{"localhost:9095"};
    options3.setIsTestKafka(true);
    options3.setAuth(KafkaAuthOptions::parse(IDLParserContext("KafkaAuthOptions"), fromjson(R"({
        "saslUsername": "user12345",
        "saslPassword": "foo1234567",
        "saslMechanism": "PLAIN"
    })")));
    kafka3.setOptions(options3.toBSON());
    kafka3.setType(ConnectionTypeEnum::Kafka);

    _context->connections =
        stdx::unordered_map<std::string, Connection>{{kafka1.getName().toString(), kafka1},
                                                     {kafka2.getName().toString(), kafka2},
                                                     {kafka3.getName().toString(), kafka3}};
    auto inMemoryConnection = testInMemoryConnectionRegistry();
    _context->connections.insert(inMemoryConnection.begin(), inMemoryConnection.end());
    _context->streamProcessorId = streamProcessorId;

    struct ExpectedResults {
        std::string bootstrapServers;
        std::string topicName;
        bool hasTimestampExtractor = false;
        std::string timestampOutputFieldName = std::string(kDefaultTsFieldName);
        int partitionCount = 1;
        int64_t startOffset{RdKafka::Topic::OFFSET_END};
        BSONObj auth;
        std::string consumerGroupId{fmt::format("asp-{}-consumer", streamProcessorId)};
    };

    auto innerTest = [&](const BSONObj& spec, const ExpectedResults& expected) {
        // Parse the pipeline.
        TumblingWindowOptions windowOptions(
            StreamTimeDuration{1, StreamTimeUnitEnum::Second},
            std::vector<mongo::BSONObj>{BSON("$match" << BSON("a" << 1))});
        std::vector<BSONObj> pipeline{
            spec, BSON("$tumblingWindow" << windowOptions.toBSON()), emitStage()};
        Planner planner{_context.get(), /*options*/ {}};
        auto dag = planner.plan(pipeline);
        dag->start();

        auto kafkaOperator = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
        const auto& options = kafkaOperator->getOptions();

        // Verify that all the parsed options match what is expected.
        ASSERT_EQ(expected.bootstrapServers, options.bootstrapServers);
        ASSERT_EQ(expected.topicName, options.topicName);
        ASSERT_EQ(expected.consumerGroupId, options.consumerGroupId);
        ASSERT_EQ(expected.startOffset, options.startOffset);
        ASSERT_TRUE(dynamic_cast<JsonEventDeserializer*>(options.deserializer) != nullptr);
        auto timestampExtractor = options.timestampExtractor;
        ASSERT_EQ(expected.hasTimestampExtractor, (timestampExtractor != nullptr));
        ASSERT_EQ(expected.timestampOutputFieldName, options.timestampOutputFieldName);
        ASSERT(options.useWatermarks);
        // Validate the expected auth related fields.
        ASSERT_EQ(expected.auth.getFieldNames<stdx::unordered_set<std::string>>().size(),
                  options.authConfig.size());
        const stdx::unordered_map<std::string, std::string> mapping{
            {"saslUsername", "sasl.username"},
            {"saslPassword", "sasl.password"},
            {"saslMechanism", "sasl.mechanism"},
            {"saslJaasConfig", "sasl.jaas.config"},
            {"securityProtocol", "security.protocol"},
        };
        for (const auto& authField : expected.auth) {
            std::string fieldName = mapping.at(authField.fieldName());
            ASSERT_EQ(authField.String(), options.authConfig.at(fieldName));
        }
        dag->stop();

        // Validate that, without a window, there are no watermark generators.
        std::vector<BSONObj> pipelineWithoutWindow{spec, emitStage()};
        dag = planner.plan(pipelineWithoutWindow);
        dag->start();
        kafkaOperator = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
        ASSERT(!kafkaOperator->getOptions().useWatermarks);
        for (int i = 0; i < expected.partitionCount; i++) {
            ASSERT_EQ(nullptr, getConsumerInfo(kafkaOperator, i).watermarkGenerator.get());
        }
        dag->stop();
    };

    auto topicName = "topic1";
    innerTest(BSON("$source" << BSON("connectionName" << kafka1.getName() << "topic" << topicName
                                                      << "testOnlyPartitionCount" << 1)),
              {options1.getBootstrapServers().toString(), topicName});
    innerTest(BSON("$source" << BSON("connectionName" << kafka3.getName() << "topic" << topicName
                                                      << "testOnlyPartitionCount" << 1)),
              {.bootstrapServers = options3.getBootstrapServers().toString(),
               .topicName = topicName,
               .auth = options3.getAuth()->toBSON()});
    innerTest(BSON("$source" << BSON("connectionName" << kafka1.getName() << "topic" << topicName
                                                      << "testOnlyPartitionCount" << 1 << "config"
                                                      << BSON("group_id"
                                                              << "consumer-group-1"))),
              {.bootstrapServers = options1.getBootstrapServers().toString(),
               .topicName = topicName,
               .consumerGroupId = "consumer-group-1"});

    auto tsField = "_tsOverride";

    auto partitionCount = 3;
    auto topic2 = "topic2";
    innerTest(BSON("$source" << BSON(
                       "connectionName"
                       << kafka2.getName() << "topic" << topic2 << "timeField"
                       << BSON("$toDate" << BSON("$multiply"
                                                 << BSONArrayBuilder().append("").append(5).arr()))
                       << "tsFieldOverride" << tsField << "testOnlyPartitionCount"
                       << partitionCount)),
              {options2.getBootstrapServers().toString(),
               topic2,
               true,
               tsField,
               partitionCount,
               RdKafka::Topic::OFFSET_END,
               options2.getAuth()->toBSON()});

    auto autoOffsetResetTest = [&](std::string autoOffsetReset, int64_t expectedOffset) {
        innerTest(
            BSON("$source" << BSON(
                     "connectionName"
                     << kafka2.getName() << "topic" << topic2 << "timeField"
                     << BSON("$toDate"
                             << BSON("$multiply" << BSONArrayBuilder().append("").append(5).arr()))
                     << "tsFieldOverride" << tsField << "testOnlyPartitionCount" << partitionCount
                     << "config" << BSON("auto_offset_reset" << autoOffsetReset))),
            {options2.getBootstrapServers().toString(),
             topic2,
             true,
             tsField,
             partitionCount,
             expectedOffset,
             options2.getAuth()->toBSON()});
    };

    std::vector<std::pair<std::string, int64_t>> testCases{
        {"smallest", RdKafka::Topic::OFFSET_BEGINNING},
        {"earliest", RdKafka::Topic::OFFSET_BEGINNING},
        {"beginning", RdKafka::Topic::OFFSET_BEGINNING},
        {"largest", RdKafka::Topic::OFFSET_END},
        {"latest", RdKafka::Topic::OFFSET_END},
        {"end", RdKafka::Topic::OFFSET_END}};

    // Missing `KafkaSourceAutoOffsetReset` values.
    ASSERT_EQUALS(idlEnumCount<KafkaSourceAutoOffsetResetEnum>, testCases.size());
    for (const auto& [input, expected] : testCases) {
        autoOffsetResetTest(input, expected);
    }
}

/**
 * Verfy that we can parse a change streams $source as follows:
 * See stages.idl
          { $source: {
            connectionName: string,
            db: optional<string>,
            coll: optional<string>,
            timeField: optional<object>,
            tsFieldOverride: optional<string>,
            startAfter:  optional<resumeToken>,
            startAtOperationTime: optional<timestamp>,
            fullDocument: fullDocumentMode,
        }}
 */
TEST_F(PlannerTest, ChangeStreamsSource) {
    Connection changeStreamConn;
    changeStreamConn.setName("myconnection");
    AtlasConnectionOptions options;
    const std::string kUriString = "mongodb://localhost:1234";
    options.setUri(kUriString);
    changeStreamConn.setOptions(options.toBSON());
    changeStreamConn.setType(mongo::ConnectionTypeEnum::Atlas);
    _context->connections = stdx::unordered_map<std::string, Connection>{
        {changeStreamConn.getName().toString(), changeStreamConn}};

    struct ExpectedResults {
        std::string expectedUri;
        bool hasTimestampExtractor = false;
        std::string expectedTimestampOutputFieldName = std::string(kDefaultTsFieldName);

        std::string expectedDatabase;
        std::string expectedCollection;
        mongocxx::pipeline expectedChangeStreamPipeline;
        boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>> expectedStartingPoint;
        mongo::FullDocumentModeEnum expectedFullDocumentMode{mongo::FullDocumentModeEnum::kDefault};
    };

    auto checkExpectedResults = [&](const BSONObj& spec, const ExpectedResults& expectedResults) {
        std::vector<BSONObj> pipeline{spec, emitStage()};
        Planner planner{_context.get(), /*options*/ {}};
        auto dag = planner.plan(pipeline);
        auto changeStreamOperator =
            dynamic_cast<ChangeStreamSourceOperator*>(dag->operators().front().get());

        // Assert that we have a change stream $source operator.
        ASSERT(changeStreamOperator);

        // Verify that all the parsed options match what is expected.
        const ChangeStreamSourceOperator::Options& options =
            static_cast<const ChangeStreamSourceOperator::Options&>(
                changeStreamOperator->getOptions());

        // uri
        ASSERT_EQ(expectedResults.expectedUri, options.clientOptions.uri);

        // timeField
        auto timestampExtractor = options.timestampExtractor;
        ASSERT_EQ(expectedResults.hasTimestampExtractor, (timestampExtractor != nullptr));

        // tsFieldOverride
        ASSERT_EQ(expectedResults.expectedTimestampOutputFieldName,
                  options.timestampOutputFieldName);

        // nss components
        ASSERT_EQ(expectedResults.expectedDatabase, options.clientOptions.database);
        ASSERT_EQ(expectedResults.expectedCollection, options.clientOptions.collection);

        // Starting point variant.
        ASSERT_EQ(bool(expectedResults.expectedStartingPoint),
                  bool(options.userSpecifiedStartingPoint));
        if (expectedResults.expectedStartingPoint) {
            ASSERT_EQ(expectedResults.expectedStartingPoint->index(),
                      options.userSpecifiedStartingPoint->index());
        }

        // FullDocumentMode for update events.
        ASSERT_EQ(expectedResults.expectedFullDocumentMode, options.fullDocumentMode);

        // TODO The cxx driver does NOT offer a way to access 'start_at_operation_time'. As such, we
        // cannot test for this option.
    };

    ExpectedResults results;
    results.expectedUri = kUriString;
    results.expectedDatabase = "db";
    results.expectedCollection = "foo";
    results.expectedChangeStreamPipeline = mongocxx::pipeline();

    // Basic parsing case.
    checkExpectedResults(fromjson("{'$source': {'connectionName': 'myconnection', 'db': 'db', "
                                  "'coll': 'foo'}}"),
                         results);

    // Configure some options common to different $source operators.
    results.expectedTimestampOutputFieldName = std::string("otherTimeFieldOutput");
    results.hasTimestampExtractor = true;
    checkExpectedResults(fromjson("{'$source': {'connectionName': 'myconnection', 'db': 'db', "
                                  "'coll': 'foo', 'timeField': {$toDate: '$a'}, 'tsFieldOverride': "
                                  "'otherTimeFieldOutput'}}"),
                         results);

    // Reset 'expectedTimestampOutputFieldName' and 'hasTimestampExtractor'.
    results.expectedTimestampOutputFieldName = std::string(kDefaultTsFieldName);
    results.hasTimestampExtractor = false;

    // Configure options specific to change streams $source.

    // Create a resume token.
    const BSONObj sampleResumeToken = fromjson(
        "{'_data':'"
        "826470FAD4000000152B042C0100296E5A1004E13815DACBED4169A6BBBC55398347EF463C6F7065726174696F"
        "6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B0C000004','_typeBits':"
        "{'$binary':'goAA','$type':'00'}}");

    // Configure 'startAfter'.
    results.expectedStartingPoint = sampleResumeToken;
    checkExpectedResults(
        fromjson("{'$source': {'connectionName': 'myconnection', 'db': 'db', 'coll': 'foo', "
                 "'config': { 'startAfter': "
                 "{'_data':'"
                 "826470FAD4000000152B042C0100296E5A1004E13815DACBED4169A6BBBC55398347EF463C6F70657"
                 "26174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B0C00"
                 "0004', '_typeBits': { '$binary': 'goAA', '$type': '00' }}}}}"),
        results);

    // Configure 'fullDocument' with the four valid values.
    results.expectedStartingPoint = boost::none;
    for (const auto& fullDocumentValue :
         std::vector<std::string>{"default", "updateLookup", "whenAvailable", "required"}) {
        results.expectedFullDocumentMode =
            FullDocumentMode_parse(IDLParserContext("test"), fullDocumentValue);
        const auto actualSpec =
            "{'$source': {'connectionName': 'myconnection', 'db': 'db', 'coll': 'foo', "
            "'config': { 'fullDocument': '" +
            fullDocumentValue + "'}}}";
        checkExpectedResults(fromjson(actualSpec), results);
    }
}

TEST_F(PlannerTest, EphemeralSink) {
    _context->connections = testInMemoryConnectionRegistry();
    Planner planner(_context.get(), /*options*/ {});
    // A pipeline without a sink.
    std::vector<BSONObj> pipeline{sourceStage()};
    // For typical non-ephemeral pipelines, we don't allow this.
    ASSERT_THROWS_CODE(planner.plan(pipeline), DBException, (int)ErrorCodes::InvalidOptions);

    // If ephemeral=true is supplied in start, we allow a pipeline without a sink.
    _context->isEphemeral = true;
    auto dag = planner.plan(pipeline);
    dag->start();

    const auto& ops = dag->operators();
    ASSERT_GTE(ops.size(), 2);
    // Verify the dummy sink operator is created.
    auto sink = dynamic_cast<NoOpSinkOperator*>(ops[1].get());
    ASSERT(sink);
    auto source = dynamic_cast<InMemorySourceOperator*>(ops[0].get());
    ASSERT(source);

    source->start();
    sink->start();

    source->addDataMsg(StreamDataMsg{.docs = {StreamDocument{Document{BSON("a" << 1)}},
                                              StreamDocument{Document{BSON("a" << 1)}}}});
    source->runOnce();
    ASSERT_EQ(2, sink->getStats().numInputDocs);
    dag->stop();
}

/**
 * Verifies we can parse the Kafka emit spec.
 * See stages.idl
        { $emit: {
            connectionName: string,
            topic: string,
        }},
 */
TEST_F(PlannerTest, KafkaEmitParsing) {
    Connection kafka1;
    const auto connName = "myConnection";
    kafka1.setName(connName);
    KafkaConnectionOptions options1{"localhost:9092"};
    options1.setIsTestKafka(true);
    options1.setAuth(KafkaAuthOptions::parse(IDLParserContext("KafkaAuthOptions"), fromjson(R"({
        "saslUsername": "user123",
        "saslPassword": "foo12345",
        "saslMechanism": "PLAIN",
        "securityProtocol": "SASL_PLAINTEXT"
    })")));
    kafka1.setOptions(options1.toBSON());
    kafka1.setType(ConnectionTypeEnum::Kafka);

    _context->connections =
        stdx::unordered_map<std::string, Connection>{{kafka1.getName().toString(), kafka1}};
    auto inMemoryConnection = testInMemoryConnectionRegistry();
    _context->connections.insert(inMemoryConnection.begin(), inMemoryConnection.end());

    struct ExpectedResults {
        std::string bootstrapServers;
        std::string topicName;
        BSONObj auth;
    };

    std::vector<BSONObj> rawPipeline{getTestSourceSpec(), fromjson(R"(
            {
                $emit: {connectionName: 'myConnection', topic: 'myOutputTopic' }
            }
        )")};

    ExpectedResults expected{
        options1.getBootstrapServers().toString(), "myOutputTopic", options1.getAuth()->toBSON()};

    Planner planner(_context.get(), /*options*/ {});
    auto dag = planner.plan(rawPipeline);
    const auto& ops = dag->operators();

    ASSERT_EQ(ops.size(), 2);
    auto kafkaEmitOperator = dynamic_cast<KafkaEmitOperator*>(dag->operators().back().get());
    ASSERT(kafkaEmitOperator);
    auto options = kafkaEmitOperator->getOptions();
    ASSERT_EQ(expected.bootstrapServers, options.bootstrapServers);
    ASSERT_EQ(expected.topicName, options.topicName.getLiteral());

    // Validate the expected auth related fields.
    ASSERT_EQ(expected.auth.getFieldNames<stdx::unordered_set<std::string>>().size(),
              options.authConfig.size());
    const stdx::unordered_map<std::string, std::string> mapping{
        {"saslUsername", "sasl.username"},
        {"saslPassword", "sasl.password"},
        {"saslMechanism", "sasl.mechanism"},
        {"securityProtocol", "security.protocol"},
    };

    for (const auto& authField : expected.auth) {
        std::string fieldName = mapping.at(authField.fieldName());
        ASSERT_EQ(authField.String(), options.authConfig.at(fieldName));
    }

    dag->stop();
}

TEST_F(PlannerTest, OperatorId) {
    // So a single $source is allowed.
    _context->isEphemeral = true;

    struct TestSpec {
        std::vector<BSONObj> pipeline;
        // Expected number of "main" operators in the top level pipeline.
        int32_t expectedMainOperators{0};
    };
    auto innerTest = [&](TestSpec spec) {
        _context->connections = testInMemoryConnectionRegistry();
        Planner planner(_context.get(), {});
        std::vector<BSONObj> pipeline{spec.pipeline};
        auto dag = planner.plan(pipeline);
        auto& ops = dag->operators();
        ASSERT_EQ(spec.expectedMainOperators, ops.size());
        int32_t operatorId = 0;
        for (int32_t opId = 0; opId < spec.expectedMainOperators; ++opId) {
            auto& op = ops[opId];
            // Verify the Operator ID.
            ASSERT_EQ(operatorId++, op->getOperatorId());
        }
    };

    // Verify a $source only pipeline. A dummy sink is created in this case.
    innerTest({{sourceStage()}, 2});
    // Verify a $source,$emit pipeline.
    innerTest({{sourceStage(), emitStage()}, 2});
    // Verify a pipeline with a variable number of $addFields stages in between the $source and
    // $emit.
    for (auto countStages : std::vector<int>{0, 1, 10, 200}) {
        TestSpec spec;
        spec.pipeline.push_back(sourceStage());
        for (int i = 0; i < countStages; ++i) {
            spec.pipeline.push_back(addFieldsStage(i));
        }
        spec.pipeline.push_back(emitStage());
        // The number of main operators is countStages plus the source and sink.
        spec.expectedMainOperators = countStages + 2;
        innerTest(spec);
    }
    // Verify pipelines with windows.
    innerTest(
        {.pipeline = {sourceStage(),
                      BSON("$hoppingWindow" << BSON(
                               "interval" << fromjson(R"({ size: 3, unit: "second" })") << "hopSize"
                                          << fromjson(R"({ size: 1, unit: "second"})") << "pipeline"
                                          << std::vector<BSONObj>({
                                                 addFieldsStage(0),
                                                 groupStage(),
                                                 sortStage(),
                                                 limitStage(),
                                             }))),
                      emitStage()},
         .expectedMainOperators = 5});
    innerTest({.pipeline = {sourceStage(),
                            addFieldsStage(0),
                            fromjson(R"(
                                { $hoppingWindow: {
                                    interval: { size: 3, unit: "second" },
                                    hopSize: { size: 1, unit: "second" },
                                    pipeline: [
                                        { $group: {
                                            _id: null,
                                            sum: { $sum: "$field" }
                                        }}
                                    ]
                                }}
                            )"),
                            addFieldsStage(0),
                            emitStage()},
               .expectedMainOperators = 5});
    // Verify an inner pipeline with a variable number of stages in between the $source and $emit.
    for (auto countStages : std::vector<int>{1, 10, 200}) {
        for (auto stagesBefore : std::vector<int>{1, 10, 50}) {
            for (auto stagesAfter : std::vector<int>{1, 10, 50}) {
                TestSpec spec;
                // Add the source stage.
                spec.pipeline.push_back(sourceStage());
                // Add stages before the window.
                for (int i = 0; i < stagesBefore; ++i) {
                    spec.pipeline.push_back(addFieldsStage(i));
                }
                // Add the window stage.
                std::vector<BSONObj> innerPipeline;
                for (int i = 0; i < countStages; ++i) {
                    innerPipeline.push_back(groupStage());
                }
                spec.pipeline.push_back(
                    BSON("$tumblingWindow" << BSON("interval" << BSON("size" << 3 << "unit"
                                                                             << "second")
                                                              << "pipeline" << innerPipeline)));
                // Add stages after the window.
                for (int i = 0; i < stagesAfter; ++i) {
                    spec.pipeline.push_back(addFieldsStage(i));
                }
                // Add the sink stage.
                spec.pipeline.push_back(emitStage());
                // Verify the results.
                spec.expectedMainOperators = 2 + stagesBefore + stagesAfter + countStages;
                innerTest(spec);
            }
        }
    }
    // One test that doesn't depend on the innerTest and helper methods.
    auto pipeline = R"(
[
    {
        $source: {
            connectionName: "kafka1",
            topic: "topic1",
            testOnlyPartitionCount: 5
        }
    },
    {
        $project: {
            a: 1
        }
    },
    {
        $tumblingWindow: {
            interval: {size: 5, unit: "second"},
            pipeline: [
                { $match: { "b" : 12 }},
                { $group: { _id : null, sum: {$sum: "$a"} }},
                { $sort: { "a" : 1 }},
                { $limit: 5 }
            ]
        }
    },
    {
        $merge: {
            into: {
                connectionName: "atlas1",
                db: "test1",
                coll: "test1"
            }
        }
    }
]
    )";
    auto bson = parsePipeline(pipeline);
    KafkaConnectionOptions options1{"localhost:9092"};
    options1.setIsTestKafka(true);
    _context->connections = stdx::unordered_map<std::string, Connection>{
        {"kafka1", Connection{"kafka1", ConnectionTypeEnum::Kafka, options1.toBSON()}},
        {"atlas1",
         Connection{"atlas1",
                    ConnectionTypeEnum::Atlas,
                    AtlasConnectionOptions{"mongodb://localhost"}.toBSON()}}};
    Planner planner(_context.get(), Planner::Options{});
    auto dag = planner.plan(bson);
    ASSERT_EQ(0, dag->operators()[0]->getOperatorId());
    ASSERT_EQ("KafkaConsumerOperator", dag->operators()[0]->getName());
    ASSERT_EQ(1, dag->operators()[1]->getOperatorId());
    ASSERT_EQ("ProjectOperator", dag->operators()[1]->getName());
    ASSERT_EQ(2, dag->operators()[2]->getOperatorId());
    ASSERT_EQ("MatchOperator", dag->operators()[2]->getName());
    ASSERT_EQ(3, dag->operators()[3]->getOperatorId());
    ASSERT_EQ("GroupOperator", dag->operators()[3]->getName());
    // The sort, limit is optimized into a single SortLimit documentsource which is a single
    // SortOperator.
    ASSERT_EQ("SortOperator", dag->operators()[4]->getName());
    ASSERT_EQ(4, dag->operators()[4]->getOperatorId());
    ASSERT_EQ("MergeOperator", dag->operators()[5]->getName());
    ASSERT_EQ(5, dag->operators()[5]->getOperatorId());
}

}  // namespace
}  // namespace streams
