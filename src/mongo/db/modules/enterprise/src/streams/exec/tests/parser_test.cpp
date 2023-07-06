/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <algorithm>
#include <chrono>
#include <iostream>
#include <rdkafkacpp.h>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonmisc.h"
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
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/noop_sink_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/parser.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/test_constants.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"
#include "streams/exec/window_operator.h"
#include "streams/util/metric_manager.h"

namespace streams {

using std::string;
using std::tuple;
using std::vector;
using namespace mongo;

class ParserTest : public AggregationContextFixture {
public:
    ParserTest() {
        _metricManager = std::make_unique<MetricManager>();
        _context = getTestContext(/*svcCtx*/ nullptr, _metricManager.get());
    }

    std::unique_ptr<OperatorDag> addSourceSinkAndParse(vector<BSONObj> rawPipeline) {
        Parser parser(_context.get(), /*options*/ {}, /*connections*/ {});
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

    int64_t getAllowedLateness(DelayedWatermarkGenerator* watermarkGenerator) {
        return watermarkGenerator->_allowedLatenessMs;
    }

    KafkaConsumerOperator::ConsumerInfo& getConsumerInfo(
        KafkaConsumerOperator* kafkaConsumerOperator, size_t idx) {
        return kafkaConsumerOperator->_consumers[idx];
    }

    const WindowOperator::Options& getWindowOptions(WindowOperator* window) {
        return window->_options;
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
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

    Parser parser(_context.get(), /*options*/ {}, connections);
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
        Parser parser(_context.get(), /*options*/ {}, /*connections*/ {});
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
 * See stages.idl
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
    options3.setAuth(KafkaAuthOptions::parse(IDLParserContext("KafkaAuthOptions"), fromjson(R"({
        "saslUsername": "user12345",
        "saslPassword": "foo1234567",
        "saslMechanism": "PLAIN"
    })")));
    kafka3.setOptions(options3.toBSON());
    kafka3.setType(ConnectionTypeEnum::Kafka);

    stdx::unordered_map<std::string, Connection> connections{{kafka1.getName().toString(), kafka1},
                                                             {kafka2.getName().toString(), kafka2},
                                                             {kafka3.getName().toString(), kafka3}};

    struct ExpectedResults {
        std::string bootstrapServers;
        std::string topicName;
        bool hasTimestampExtractor = false;
        std::string timestampOutputFieldName = string(Parser::kDefaultTsFieldName);
        int partitionCount = 1;
        StreamTimeDuration allowedLateness{3, StreamTimeUnitEnum::Second};
        int64_t startOffset{RdKafka::Topic::OFFSET_END};
        BSONObj auth;
    };

    auto innerTest = [&](const BSONObj& spec, const ExpectedResults& expected) {
        // Parse the pipeline.
        TumblingWindowOptions windowOptions(
            StreamTimeDuration{1, StreamTimeUnitEnum::Second},
            std::vector<mongo::BSONObj>{BSON("$match" << BSON("a" << 1))});
        std::vector<BSONObj> pipeline{
            spec, BSON("$tumblingWindow" << windowOptions.toBSON()), emitStage()};
        Parser parser{_context.get(), /*options*/ {}, connections};
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
        ASSERT(options.useWatermarks);
        for (int i = 0; i < expected.partitionCount; i++) {
            ASSERT_EQ(i, options.partitionOptions[i].partition);
            ASSERT_EQ(expected.startOffset, options.partitionOptions[i].startOffset);
            auto size = expected.allowedLateness.getSize();
            auto unit = expected.allowedLateness.getUnit();
            auto millis = toMillis(unit, size);
            ASSERT_EQ(millis,
                      getAllowedLateness(dynamic_cast<DelayedWatermarkGenerator*>(
                          getConsumerInfo(kafkaOperator, i).watermarkGenerator.get())));
        }
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

        // Validate that, without a window, there are no watermark generators.
        std::vector<BSONObj> pipelineWithoutWindow{spec, emitStage()};
        dag = parser.fromBson(pipelineWithoutWindow);
        kafkaOperator = dynamic_cast<KafkaConsumerOperator*>(dag->operators().front().get());
        ASSERT(!kafkaOperator->getOptions().useWatermarks);
        for (int i = 0; i < expected.partitionCount; i++) {
            ASSERT_EQ(nullptr, getConsumerInfo(kafkaOperator, i).watermarkGenerator.get());
        }
    };

    auto topicName = "topic1";
    innerTest(BSON("$source" << BSON("connectionName" << kafka1.getName() << "topic" << topicName
                                                      << "partitionCount" << 1)),
              {options1.getBootstrapServers().toString(), topicName});
    innerTest(BSON("$source" << BSON("connectionName" << kafka3.getName() << "topic" << topicName
                                                      << "partitionCount" << 1)),
              {.bootstrapServers = options3.getBootstrapServers().toString(),
               .topicName = topicName,
               .auth = options3.getAuth()->toBSON()});

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
               allowedLateness,
               RdKafka::Topic::OFFSET_END,
               options2.getAuth()->toBSON()});

    auto startAtTest = [&](std::string startAt, int64_t expectedOffset) {
        innerTest(
            BSON("$source" << BSON(
                     "connectionName"
                     << kafka2.getName() << "topic" << topic2 << "timeField"
                     << BSON("$toDate"
                             << BSON("$multiply" << BSONArrayBuilder().append("").append(5).arr()))
                     << "tsFieldOverride" << tsField << "allowedLateness"
                     << allowedLateness.toBSON() << "partitionCount" << partitionCount << "config"
                     << BSON("startAt" << startAt))),
            {options2.getBootstrapServers().toString(),
             topic2,
             true,
             tsField,
             partitionCount,
             allowedLateness,
             expectedOffset,
             options2.getAuth()->toBSON()});
    };
    startAtTest("latest", RdKafka::Topic::OFFSET_END);
    startAtTest("earliest", RdKafka::Topic::OFFSET_BEGINNING);
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
            allowedLateness: optional<object>,
            resumeAfter: optional<resumeToken>,
            startAfter:  optional<resumeToken>,
            startAtOperationTime: optional<timestamp>,
        }}
 */
TEST_F(ParserTest, ChangeStreamsSource) {
    Connection changeStreamConn;
    changeStreamConn.setName("myconnection");
    AtlasConnectionOptions options;
    const std::string kUriString = "mongodb://localhost:1234";
    options.setUri(kUriString);
    changeStreamConn.setOptions(options.toBSON());
    changeStreamConn.setType(mongo::ConnectionTypeEnum::Atlas);
    stdx::unordered_map<std::string, Connection> connections{
        {changeStreamConn.getName().toString(), changeStreamConn}};

    struct ExpectedResults {
        std::string expectedUri;
        bool hasTimestampExtractor = false;
        StreamTimeDuration expectedAllowedLateness{3, StreamTimeUnitEnum::Second};
        std::string expectedTimestampOutputFieldName = string(Parser::kDefaultTsFieldName);

        mongo::NamespaceString expectedNss;
        mongocxx::pipeline expectedChangeStreamPipeline;
        mongocxx::options::change_stream expectedChangeStreamOptions;
    };

    auto checkExpectedResults = [&](const BSONObj& spec, const ExpectedResults& expectedResults) {
        std::vector<BSONObj> pipeline{spec, emitStage()};
        Parser parser{_context.get(), /*options*/ {}, connections};
        auto dag = parser.fromBson(pipeline);
        auto changeStreamOperator =
            dynamic_cast<ChangeStreamSourceOperator*>(dag->operators().front().get());

        // Assert that we have a change stream $source operator.
        ASSERT(changeStreamOperator);

        // Verify that all the parsed options match what is expected.
        const auto& options = changeStreamOperator->getOptions();

        // uri
        ASSERT_EQ(expectedResults.expectedUri, options.uri);

        // timeField
        auto timestampExtractor = options.timestampExtractor;
        ASSERT_EQ(expectedResults.hasTimestampExtractor, (timestampExtractor != nullptr));

        // tsFieldOverride
        ASSERT_EQ(expectedResults.expectedTimestampOutputFieldName,
                  options.timestampOutputFieldName);

        // nss
        ASSERT_EQ(expectedResults.expectedNss, options.nss);

        // Change stream options
        const auto& expected = expectedResults.expectedChangeStreamOptions;
        const auto& actual = options.changeStreamOptions;

        if (expected.resume_after().has_value()) {
            ASSERT_EQ(expected.resume_after(), actual.resume_after());
        }

        if (expected.start_after().has_value()) {
            ASSERT_EQ(expected.start_after(), actual.start_after());
        }

        // TODO The cxx driver does NOT offer a way to access 'start_at_operation_time'. As such, we
        // cannot test for this option.
    };

    ExpectedResults results;
    results.expectedUri = kUriString;
    results.expectedNss = NamespaceString("db", "foo", boost::none /* tenantId */);
    results.expectedChangeStreamPipeline = mongocxx::pipeline();
    mongocxx::options::change_stream changeStreamOptions;
    results.expectedChangeStreamOptions = changeStreamOptions;

    // Basic parsing case.
    checkExpectedResults(fromjson("{'$source': {'connectionName': 'myconnection', 'db': 'db', "
                                  "'coll': 'foo'}}"),
                         results);

    // Configure some options common to different $source operators.
    results.expectedTimestampOutputFieldName = std::string("otherTimeFieldOutput");
    results.hasTimestampExtractor = true;
    checkExpectedResults(
        fromjson("{'$source': {'connectionName': 'myconnection', 'db': 'db', "
                 "'coll': 'foo', 'timeField': {$toDate: '$a'}, 'tsFieldOverride': "
                 "'otherTimeFieldOutput', 'allowedLateness': {'size': 3, 'unit': 'second'}}}"),
        results);

    // Reset 'expectedTimestampOutputFieldName' and 'hasTimestampExtractor'.
    results.expectedTimestampOutputFieldName = string(Parser::kDefaultTsFieldName);
    results.hasTimestampExtractor = false;

    // Configure options specific to change streams $source.

    // Create a resume token.
    const BSONObj sampleResumeToken = fromjson(
        "{'_data':'"
        "826470FAD4000000152B042C0100296E5A1004E13815DACBED4169A6BBBC55398347EF463C6F7065726174696F"
        "6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B0C000004','_typeBits':"
        "{'$binary':'goAA','$type':'00'}}");

    // Stash a copy of 'changeStreamOptions' so that we can reset to this state.
    const auto originalOptions = changeStreamOptions;

    // Configure 'resumeAfter'.
    changeStreamOptions.resume_after(toBsoncxxDocument(sampleResumeToken));
    results.expectedChangeStreamOptions = changeStreamOptions;
    checkExpectedResults(
        fromjson("{'$source': {'connectionName': 'myconnection', 'db': 'db', 'coll': 'foo', "
                 "'resumeAfter': "
                 "{'_data':'"
                 "826470FAD4000000152B042C0100296E5A1004E13815DACBED4169A6BBBC55398347EF463C6F70657"
                 "26174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B0C00"
                 "0004', '_typeBits': { '$binary': 'goAA', '$type': '00' }}}}"),
        results);

    // Configure 'startAfter'. Reset 'changeStreamOptions' to 'originalOptions' to clear
    // 'resumeAfter'.
    changeStreamOptions = originalOptions;
    changeStreamOptions.start_after(toBsoncxxDocument(sampleResumeToken));
    results.expectedChangeStreamOptions = changeStreamOptions;

    // Failure cases.
    Parser parser{_context.get(), /*options*/ {}, connections};
    auto emit = emitStage();

    // Configure the mutually exclusive options 'startAfter' and 'resumeAfter'. Note that this will
    // not throw at parse time; it is expected to throw when the stream processor attempts to
    // establish a cursor.
    ASSERT_DOES_NOT_THROW(parser.fromBson(
        {fromjson(
             "{'$source': {'connectionName': 'myconnection','db': 'db', "
             "'coll': 'foo', "
             "'startAfter': "
             "{'_data':'"
             "826470FAD4000000152B042C0100296E5A1004E13815DACBED4169A6BBBC55398347EF463C6F70657"
             "26174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B0C00"
             "0004', '_typeBits': { '$binary': 'goAA', '$type': '00' }},"
             "'resumeAfter': "
             "{'_data':'"
             "826470FAD4000000152B042C0100296E5A1004E13815DACBED4169A6BBBC55398347EF463C6F70657"
             "26174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B0C00"
             "0004', '_typeBits': { '$binary': 'goAA', '$type': '00' }}}}"),
         emit}));
}

TEST_F(ParserTest, EphemeralSink) {
    Parser parser(_context.get(), /*options*/ {}, /*connections*/ {});
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

/**
 * Verifies we can parse the Kafka emit spec.
 * See stages.idl
        { $emit: {
            connectionName: string,
            topic: string,
        }},
 */
TEST_F(ParserTest, KafkaEmitParsing) {
    Connection kafka1;
    const auto connName = "myConnection";
    kafka1.setName(connName);
    KafkaConnectionOptions options1{"localhost:9092"};
    options1.setAuth(KafkaAuthOptions::parse(IDLParserContext("KafkaAuthOptions"), fromjson(R"({
        "saslUsername": "user123",
        "saslPassword": "foo12345",
        "saslMechanism": "PLAIN",
        "securityProtocol": "SASL_PLAINTEXT"
    })")));
    kafka1.setOptions(options1.toBSON());
    kafka1.setType(ConnectionTypeEnum::Kafka);

    stdx::unordered_map<std::string, Connection> connections{{kafka1.getName().toString(), kafka1}};

    struct ExpectedResults {
        std::string bootstrapServers;
        std::string topicName;
        BSONObj auth;
    };

    vector<BSONObj> rawPipeline{getTestSourceSpec(), fromjson(R"(
            {
                $emit: {connectionName: 'myConnection', topic: 'myOutputTopic' }
            }
        )")};

    ExpectedResults expected{
        options1.getBootstrapServers().toString(), "myOutputTopic", options1.getAuth()->toBSON()};

    Parser parser(_context.get(), /*options*/ {}, connections);
    auto dag = parser.fromBson(rawPipeline);
    const auto& ops = dag->operators();

    ASSERT_EQ(ops.size(), 2);
    auto kafkaEmitOperator = dynamic_cast<KafkaEmitOperator*>(dag->operators().back().get());
    ASSERT(kafkaEmitOperator);
    auto options = kafkaEmitOperator->getOptions();
    ASSERT_EQ(expected.bootstrapServers, options.bootstrapServers);
    ASSERT_EQ(expected.topicName, options.topicName);

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
}

TEST_F(ParserTest, OperatorId) {
    // So a single $source is allowed.
    _context->isEphemeral = true;

    struct TestSpec {
        std::vector<BSONObj> pipeline;
        // Expected number of "main" operators in the top level pipeline.
        int32_t expectedMainOperators{0};
        // Expected number of inner operators in the WindowOperator's inner pipeline.
        int32_t expectedInnerOperators{0};
    };
    auto innerTest = [&](TestSpec spec) {
        Parser parser(_context.get(), {});
        std::vector<BSONObj> pipeline{spec.pipeline};
        auto dag = parser.fromBson(pipeline);
        auto& ops = dag->operators();
        ASSERT_EQ(spec.expectedMainOperators, ops.size());
        int32_t operatorId = 0;
        for (int mainOperator = 0; mainOperator < spec.expectedMainOperators; ++mainOperator) {
            auto& op = ops[mainOperator];
            // Verify the Operator ID.
            ASSERT_EQ(operatorId++, op->getOperatorId());
            if (auto window = dynamic_cast<WindowOperator*>(op.get())) {
                auto innerPipeline = getWindowOptions(window).pipeline;
                Parser parser(_context.get(), {.planMainPipeline = false});
                auto parsedInnerPipeline = Pipeline::parse(innerPipeline, _context->expCtx);
                // TODO(SERVER-78478): Remove this once we're serializing an optimized
                // representation of the pipeline.
                parsedInnerPipeline->optimizePipeline();
                auto innerDag = parser.fromPipeline(*parsedInnerPipeline, operatorId);
                ASSERT_EQ(spec.expectedInnerOperators, innerDag.size());
                for (auto& op : innerDag) {
                    ASSERT_EQ(operatorId++, op->getOperatorId());
                }
                // One for the CollectOperator.
                operatorId++;
                spec.expectedInnerOperators++;
            }
        }
        // After the increments above, validate that operatorId equals the expected total number of
        // operators.
        ASSERT_EQ(spec.expectedMainOperators + spec.expectedInnerOperators, operatorId);
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
         // $source, $hoppingWindow, and $emit
         .expectedMainOperators = 3,
         // The WindowOperator's inner pipeline after optimization: [$addFields, $group,
         // $sortLimit].
         .expectedInnerOperators = 3});
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
               .expectedMainOperators = 5,
               .expectedInnerOperators = 1});
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
                // One source, one sink, one window, plus stagesBefore and stagesAfter.
                spec.expectedMainOperators = 3 + stagesBefore + stagesAfter;
                // The window's inner pipeline is countStages long.
                spec.expectedInnerOperators = countStages;
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
            partitionCount: 5
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
    Parser parser(
        _context.get(),
        Parser::Options{},
        {{"kafka1",
          Connection{
              "kafka1", ConnectionTypeEnum::Kafka, KafkaConnectionOptions{"localhost"}.toBSON()}},
         {"atlas1",
          Connection{"atlas1",
                     ConnectionTypeEnum::Atlas,
                     AtlasConnectionOptions{"mongodb://localhost"}.toBSON()}}});
    auto dag = parser.fromBson(bson);
    ASSERT_EQ(0, dag->operators()[0]->getOperatorId());
    ASSERT_EQ("KafkaConsumerOperator", dag->operators()[0]->getName());
    ASSERT_EQ(1, dag->operators()[1]->getOperatorId());
    ASSERT_EQ("ProjectOperator", dag->operators()[1]->getName());
    ASSERT_EQ(2, dag->operators()[2]->getOperatorId());
    ASSERT_EQ("WindowOperator", dag->operators()[2]->getName());
    if (auto window = dynamic_cast<WindowOperator*>(dag->operators()[2].get())) {
        Parser parser(_context.get(), {.planMainPipeline = false});
        auto parsedInnerPipeline =
            Pipeline::parse(getWindowOptions(window).pipeline, _context->expCtx);
        auto innerDag = parser.fromPipeline(*parsedInnerPipeline, 3);
        ASSERT_EQ(3, innerDag[0]->getOperatorId());
        ASSERT_EQ("MatchOperator", innerDag[0]->getName());
        ASSERT_EQ(4, innerDag[1]->getOperatorId());
        ASSERT_EQ("GroupOperator", innerDag[1]->getName());
        ASSERT_EQ(5, innerDag[2]->getOperatorId());
        // The sort, limit is optimized into a single SortLimit documentsource which is a single
        // SortOperator.
        ASSERT_EQ("SortOperator", innerDag[2]->getName());
        // OperatorID 6 is for the CollectOperator appended to the end of WindowPipeline instances.
    }
    ASSERT_EQ(7, dag->operators()[3]->getOperatorId());
    ASSERT_EQ("MergeOperator", dag->operators()[3]->getName());
}

}  // namespace streams
