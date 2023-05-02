/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <iostream>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/query/sbe_stage_builder_helpers.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/document_source_wrapper_operator.h"
#include "streams/exec/in_memory_sink_operator.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/log_sink_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/noop_sink_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/time_util.h"

namespace streams {

using std::string;
using std::tuple;
using std::vector;
using namespace mongo;

class ParserTest : public AggregationContextFixture {
public:
    ParserTest() : _context(getTestContext()) {}

    std::unique_ptr<OperatorDag> addSourceSinkAndParse(vector<BSONObj> rawPipeline) {
        Parser parser(_context.get(), {});
        if (rawPipeline.size() == 0 ||
            rawPipeline.front().firstElementFieldName() != string{"$source"}) {
            rawPipeline.insert(rawPipeline.begin(), getTestSourceSpec());
        }

        if (rawPipeline.back().firstElementFieldName() != string{"$emit"}) {
            rawPipeline.push_back(getTestLogSinkSpec());
        }

        return parser.fromBson(rawPipeline);
    }

    std::unique_ptr<OperatorDag> addSourceSinkAndParse(const std::string& pipeline) {
        return addSourceSinkAndParse(parsePipeline(pipeline));
    }

    vector<BSONObj> parsePipeline(const std::string& pipeline) {
        const auto inputBson = fromjson("{pipeline: " + pipeline + "}");
        ASSERT_EQUALS(inputBson["pipeline"].type(), BSONType::Array);
        return parsePipelineFromBSON(inputBson["pipeline"]);
    }

    BSONObj addFieldsStage(int i) {
        return BSON("$addFields" << BSON(std::to_string(i) << i));
    };

    BSONObj sourceStage() {
        return getTestSourceSpec();
    };

    BSONObj emitStage() {
        return getTestLogSinkSpec();
    };

    int64_t getAllowedLateness(const DelayedWatermarkGenerator& watermarkGenerator) {
        return watermarkGenerator._allowedLatenessMs;
    }

protected:
    std::unique_ptr<Context> _context;
};


TEST_F(ParserTest, RegularParsingErrorsWork) {
    vector<BSONObj> invalidBsonPipeline{
        BSON("$addFields" << 1),
    };
    ASSERT_THROWS_CODE(addSourceSinkAndParse(invalidBsonPipeline), AssertionException, 40272);
}

TEST_F(ParserTest, OnlySupportedStages) {
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
TEST_F(ParserTest, SupportedStagesWork1) {
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
    vector<DocumentSourceWrapperOperator*> mappers;
    for (size_t i = 1; i < ops.size() - 1; ++i) {
        ASSERT_TRUE(dynamic_cast<DocumentSourceWrapperOperator*>(ops[i].get()));
        mappers.push_back(dynamic_cast<DocumentSourceWrapperOperator*>(ops[i].get()));
    }
    ASSERT_EQ(mappers[0]->getName(), "AddFieldsOperator");
    ASSERT_EQ(mappers[1]->getName(), "MatchOperator");
    ASSERT_EQ(mappers[2]->getName(), "ProjectOperator");
    ASSERT_EQ(mappers[3]->getName(), "RedactOperator");
    ASSERT_EQ(mappers[4]->getName(), "ReplaceRootOperator");
    ASSERT_EQ(mappers[5]->getName(), "ReplaceRootOperator");  // From the user's $replaceWith.
    ASSERT_EQ(mappers[6]->getName(), "SetOperator");
    ASSERT_EQ(mappers[7]->getName(), "ProjectOperator");  // From the user's $unset.
    ASSERT_EQ(mappers[8]->getName(), "UnwindOperator");
}

/**
Put together different permutations of valid pipelines, and make sure they can
be parsed into an OpereatorDag.
We don't do much validation here on the results here,
other than "at least one operator was created" and "parsing didn't crash".
*/
TEST_F(ParserTest, SupportedStagesWork2) {
    vector<BSONObj> validStages{
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

    vector<vector<BSONObj>> validBsonPipelines;
    for (size_t i = 0; i < validStages.size(); i++) {
        for (size_t j = 0; j < validStages.size(); j++) {
            for (size_t k = 0; k < validStages.size(); k++) {
                vector<BSONObj> pipeline{validStages[i], validStages[j], validStages[k]};
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

TEST_F(ParserTest, MergeStageParsing) {
    Connection atlasConn;
    atlasConn.setName("myconnection");
    AtlasConnectionOptions atlasConnOptions{"mongodb://localhost:270"};
    atlasConn.setOptions(atlasConnOptions.toBSON());
    atlasConn.setType(ConnectionTypeEnum::Atlas);
    stdx::unordered_map<std::string, Connection> connections{
        {atlasConn.getName().toString(), atlasConn}};

    vector<BSONObj> rawPipeline{
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

    Parser parser(_context.get(), connections);
    auto dag = parser.fromBson(rawPipeline);
    const auto& ops = dag->operators();
    ASSERT_GTE(ops.size(), 1);
}

/**
 * Verify that we're taking advantage of the pipeline->optimize logic.
 * The two $match stages should be merged into one.
 */
TEST_F(ParserTest, StagesOptimized) {
    vector<BSONObj> pipeline{BSON("$addFields" << BSON("a" << 1)),
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

TEST_F(ParserTest, InvalidPipelines) {
    auto validStage = [](int i) {
        return BSONObj{BSON("$addFields" << BSON(std::to_string(i) << i))};
    };

    vector<vector<BSONObj>> pipelines{vector<BSONObj>{},
                                      vector<BSONObj>{validStage(0)},
                                      vector<BSONObj>{sourceStage(), validStage(0)},
                                      vector<BSONObj>{validStage(0), emitStage()},
                                      vector<BSONObj>{validStage(0), sourceStage(), emitStage()},
                                      vector<BSONObj>{emitStage(), validStage(0), sourceStage()}};
    for (const auto& pipeline : pipelines) {
        Parser parser(_context.get(), {});
        ASSERT_THROWS_CODE(parser.fromBson(pipeline), DBException, ErrorCodes::InvalidOptions);
    }
}

/**
 * Verify that the operators in the parsed OperatorDag are in the correct order, according to the
 * user pipeline.
 */
TEST_F(ParserTest, OperatorOrder) {
    vector<int> numStages{0, 2, 10, 100};
    const string field{"a"};
    for (int numStage : numStages) {
        vector<BSONObj> pipeline;
        for (int i = 0; i < numStage; i++) {
            pipeline.push_back(BSON("$addFields" << BSON(field << i)));
        }
        std::unique_ptr<OperatorDag> dag = addSourceSinkAndParse(pipeline);
        ASSERT_EQ(dag->operators().size(), numStage + 2);
        for (int i = 0; i < numStage; i += 1) {
            auto& op = dag->operators()[i + 1];
            ASSERT_EQ(op->getName(), "AddFieldsOperator");
            auto& wrapper = dynamic_cast<DocumentSourceWrapperOperator&>(*op);
            DocumentSource& processor = wrapper.processor();

            std::vector<Value> ser;
            processor.serializeToArray(ser);
            ASSERT_EQ(ser.size(), 1);
            const Value actual = ser[0]["$addFields"][field]["$const"];
            ASSERT_EQ(actual.getInt(), i);
        }
    }
}

/**
 * Verifies we can parse the Kafka source spec
 * See source_stage.idl
        { $source: {
            connectionName: string,
            topic: string
            timeField: optional<object>,
            tsFieldOverride: optional<string>,
            allowedLateness: optional<int>,
            partitionCount: optional<int>,
        }},
 */
TEST_F(ParserTest, KafkaSourceParsing) {
    Connection kafka1;
    kafka1.setName("myconnection");
    KafkaConnectionOptions options1{"localhost:9092"};
    kafka1.setOptions(options1.toBSON());
    kafka1.setType(ConnectionTypeEnum::Kafka);

    Connection kafka2;
    kafka2.setName("myconnection2");
    KafkaConnectionOptions options2{"localhost:9093"};
    kafka2.setOptions(options2.toBSON());
    kafka2.setType(ConnectionTypeEnum::Kafka);

    stdx::unordered_map<std::string, Connection> connections{{kafka1.getName().toString(), kafka1},
                                                             {kafka2.getName().toString(), kafka2}};

    struct ExpectedResults {
        std::string bootstrapServers;
        std::string topicName;
        bool hasTimestampExtractor = false;
        std::string timestampOutputFieldName = string(Parser::kDefaultTsFieldName);
        int partitionCount = 1;
        StreamTimeDuration allowedLateness{3, StreamTimeUnitEnum::Second};
    };

    auto innerTest = [&](const BSONObj& spec, const ExpectedResults& expected) {
        // Parse the pipeline.
        TumblingWindowOptions windowOptions(
            StreamTimeDuration{1, StreamTimeUnitEnum::Second},
            std::vector<mongo::BSONObj>{BSON("$match" << BSON("a" << 1))});
        std::vector<BSONObj> pipeline{
            spec, BSON("$tumblingWindow" << windowOptions.toBSON()), emitStage()};
        Parser parser{_context.get(), connections};
        auto dag = parser.fromBson(pipeline);

        auto kafkaOperator = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
        const auto& options = kafkaOperator->getOptions();

        // Verify that all the parsed options match what is expected.
        ASSERT_EQ(expected.bootstrapServers, options.bootstrapServers);
        ASSERT_EQ(expected.topicName, options.topicName);
        ASSERT_TRUE(dynamic_cast<JsonEventDeserializer*>(options.deserializer) != nullptr);
        auto timestampExtractor = options.timestampExtractor;
        ASSERT_EQ(expected.hasTimestampExtractor, (timestampExtractor != nullptr));
        ASSERT_EQ(expected.timestampOutputFieldName, options.timestampOutputFieldName);
        ASSERT_EQ(expected.partitionCount, options.partitionOptions.size());
        ASSERT(options.watermarkCombiner);
        for (int i = 0; i < expected.partitionCount; i++) {
            ASSERT_EQ(i, options.partitionOptions[i].partition);
            ASSERT_EQ(RdKafka::Topic::OFFSET_BEGINNING, options.partitionOptions[i].startOffset);
            auto size = expected.allowedLateness.getSize();
            auto unit = expected.allowedLateness.getUnit();
            auto millis = toMillis(unit, size);
            ASSERT_EQ(millis, getAllowedLateness(*options.partitionOptions[i].watermarkGenerator));
        }

        // Validate that, without a window, there are no watermark generators.
        std::vector<BSONObj> pipelineWithoutWindow{spec, emitStage()};
        dag = parser.fromBson(pipelineWithoutWindow);
        kafkaOperator = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
        const auto& options2 = kafkaOperator->getOptions();
        ASSERT_EQ(nullptr, options2.watermarkCombiner.get());
        for (int i = 0; i < expected.partitionCount; i++) {
            ASSERT_EQ(nullptr, options2.partitionOptions[i].watermarkGenerator.get());
        }
    };

    auto topicName = "topic1";
    innerTest(BSON("$source" << BSON("connectionName" << kafka1.getName() << "topic" << topicName
                                                      << "partitionCount" << 1)),
              {options1.getBootstrapServers().toString(), topicName});

    auto tsField = "_tsOverride";

    auto allowedLateness = StreamTimeDuration::parse(IDLParserContext("allowedLateness"),
                                                     BSON("unit"
                                                          << "second"
                                                          << "size" << 1000));
    auto partitionCount = 3;
    auto topic2 = "topic2";
    innerTest(BSON("$source" << BSON(
                       "connectionName"
                       << kafka2.getName() << "topic" << topic2 << "timeField"
                       << BSON("$toDate" << BSON("$multiply"
                                                 << BSONArrayBuilder().append("").append(5).arr()))
                       << "tsFieldOverride" << tsField << "allowedLateness"
                       << allowedLateness.toBSON() << "partitionCount" << partitionCount)),
              {options2.getBootstrapServers().toString(),
               topic2,
               true,
               tsField,
               partitionCount,
               allowedLateness});
}

TEST_F(ParserTest, EphemeralSink) {
    Parser parser(_context.get(), {});
    // A pipeline without a sink.
    std::vector<BSONObj> pipeline{sourceStage()};
    // For typical non-ephemeral pipelines, we don't allow this.
    ASSERT_THROWS_CODE(parser.fromBson(pipeline), DBException, (int)ErrorCodes::InvalidOptions);

    // If ephemeral=true is supplied in start, we allow a pipeline without a sink.
    _context->isEphemeral = true;
    auto dag = parser.fromBson(pipeline);

    const auto& ops = dag->operators();
    ASSERT_GTE(ops.size(), 2);
    // Verify the dummy sink operator is created.
    auto sink = dynamic_cast<NoOpSinkOperator*>(ops[1].get());
    ASSERT(sink);
    auto source = dynamic_cast<InMemorySourceOperator*>(ops[0].get());
    ASSERT(source);
    source->addDataMsg(StreamDataMsg{.docs = {StreamDocument{Document{BSON("a" << 1)}},
                                              StreamDocument{Document{BSON("a" << 1)}}}});
    source->runOnce();
    ASSERT_EQ(2, sink->getStats().numInputDocs);
}

}  // namespace streams
