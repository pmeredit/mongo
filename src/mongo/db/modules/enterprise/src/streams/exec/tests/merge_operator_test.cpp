/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/merge_operator.h"

#include <fmt/format.h>
#include <memory>
#include <set>

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/exec/document_value/value_comparator.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/scopeguard.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongo_process_interface_for_test.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/planner.h"
#include "streams/exec/queued_sink_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"
namespace streams {

using namespace mongo;
using namespace std::string_literals;

class MergeOperatorTest : public AggregationContextFixture {
public:
    MergeOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
    }

    void setUp() override {
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
        _context->expCtx->setMongoProcessInterface(
            std::make_shared<MongoProcessInterfaceForTest>());
        _context->dlq->registerMetrics(_executor->getMetricManager());
    }

    // Start the MergeOperator and wait for it to connect.
    void start(MergeOperator* op) {
        op->_options.isTest = true;
        op->start();
        auto deadline = Date_t::now() + Seconds{10};
        // Wait for the source to be connected like the Executor does.
        while (op->getConnectionStatus().isConnecting()) {
            stdx::this_thread::sleep_for(stdx::chrono::milliseconds(100));
            ASSERT(Date_t::now() < deadline);
        }
        ASSERT(op->getConnectionStatus().isConnected());
    }

    std::unique_ptr<MergeOperator> createMergeStage(BSONObj spec,
                                                    bool allowMergeOnNullishValues = true) {
        MutableDocument mut{Document{spec.getField("$merge").Obj()}};
        mut["into"] = Value(BSON("connectionName"
                                 << "test"
                                 << "db"
                                 << "test"
                                 << "coll"
                                 << "test"));
        MergeOperator::Options opts;
        opts.isTest = true;
        opts.allowMergeOnNullishValues = allowMergeOnNullishValues;
        auto doc = mut.freeze();
        auto bson = doc.toBson();
        opts.spec =
            MergeOperatorSpec::parseOwned(IDLParserContext{"createMergeStage"}, std::move(bson));
        return std::make_unique<MergeOperator>(_context.get(), std::move(opts));
    }

    auto makeMergeOperator(BSONObj spec) {
        auto mergeOperator = createMergeStage(std::move(spec));
        mergeOperator->registerMetrics(_executor->getMetricManager());
        start(mergeOperator.get());
        return mergeOperator;
    }

    auto getWriter(MergeOperator* op) {
        return op->_threads[0]->writer.get();
    }

    MongoProcessInterfaceForTest* getMongoProcessInterface(QueuedSinkOperator* op,
                                                           size_t threadIdx = 0) {
        auto mongo = dynamic_cast<MergeWriter*>(op->_threads[threadIdx]->writer.get())
                         ->_options.mergeExpCtx->getMongoProcessInterface();
        return dynamic_cast<MongoProcessInterfaceForTest*>(mongo.get());
    }

    std::vector<MongoProcessInterfaceForTest*> getAllMongoProcessInterfaces(
        QueuedSinkOperator* op) {
        std::vector<MongoProcessInterfaceForTest*> mongoProcessInterfaces;
        for (size_t i = 0; i < op->_threads.size(); ++i) {
            mongoProcessInterfaces.push_back(getMongoProcessInterface(op, i));
        }
        return mongoProcessInterfaces;
    }

    void setForTest(QueuedSinkOperator* op) {
        auto mongo = dynamic_cast<MergeOperator*>(op);
        mongo->_options.isTest = true;
    }

    int getNumWriters(QueuedSinkOperator* op) {
        return op->_threads.size();
    }

    auto getMergeExpCtx(MergeWriter* op) {
        return op->_options.mergeExpCtx;
    }

protected:
    std::unique_ptr<MetricManager> _metricManager;
    std::unique_ptr<Context> _context;
    std::unique_ptr<Executor> _executor;
};

// Test that {whenMatched: replace, whenNotMatched: insert} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedReplace) {
    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeOperator = createMergeStage(std::move(spec));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }
    dataMsg.creationTimer = mongo::Timer{};

    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface = getMongoProcessInterface(mergeOperator.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    ASSERT_EQUALS(10, objsUpdated.size());
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQUALS(MongoProcessInterface::UpsertType::kGenerateNewDoc, objsUpdated[i].upsert);
        ASSERT_FALSE(objsUpdated[i].multi);
        ASSERT_FALSE(objsUpdated[i].oid);
        const auto& batchObj = objsUpdated[i].batchObj;
        ASSERT_TRUE(std::get<0>(batchObj).hasField("_id"));
        ASSERT_FALSE(std::get<2>(batchObj));
        auto& updateMod = std::get<1>(batchObj);
        auto updateObj = updateMod.getUpdateReplacement();
        ASSERT_BSONOBJ_EQ(updateObj, dataMsg.docs[i].doc.toBson());
    }

    mergeOperator->stop();
}

// Test that {whenMatched: replace, whenNotMatched: discard} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedReplaceDiscard) {
    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "discard"));
    auto mergeOperator = createMergeStage(std::move(spec));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    ASSERT(mergeOperator);

    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }
    dataMsg.creationTimer = mongo::Timer{};
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface = getMongoProcessInterface(mergeOperator.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    ASSERT_EQUALS(10, objsUpdated.size());
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQUALS(MongoProcessInterface::UpsertType::kNone, objsUpdated[i].upsert);
        ASSERT_FALSE(objsUpdated[i].multi);
        ASSERT_FALSE(objsUpdated[i].oid);
        const auto& batchObj = objsUpdated[i].batchObj;
        ASSERT_TRUE(std::get<0>(batchObj).hasField("_id"));
        ASSERT_FALSE(std::get<2>(batchObj));
        auto& updateMod = std::get<1>(batchObj);
        auto updateObj = updateMod.getUpdateReplacement();
        ASSERT_BSONOBJ_EQ(updateObj, dataMsg.docs[i].doc.toBson());
    }

    mergeOperator->stop();
}

// Test that {whenMatched: keepExisting, whenNotMatched: insert} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedKeepExisting) {
    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "keepExisting"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeOperator = createMergeStage(std::move(spec));
    ASSERT(mergeOperator);
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }
    dataMsg.creationTimer = mongo::Timer{};
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface = getMongoProcessInterface(mergeOperator.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    ASSERT_EQUALS(10, objsUpdated.size());
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQUALS(MongoProcessInterface::UpsertType::kGenerateNewDoc, objsUpdated[i].upsert);
        ASSERT_FALSE(objsUpdated[i].multi);
        ASSERT_FALSE(objsUpdated[i].oid);
        const auto& batchObj = objsUpdated[i].batchObj;
        ASSERT_TRUE(std::get<0>(batchObj).hasField("_id"));
        ASSERT_FALSE(std::get<2>(batchObj));
        auto& updateMod = std::get<1>(batchObj);
        auto updateObj = updateMod.getUpdateModifier()["$setOnInsert"].Obj();
        ASSERT_BSONOBJ_EQ(updateObj, dataMsg.docs[i].doc.toBson());
    }

    mergeOperator->stop();
}

// Test that {whenMatched: fail, whenNotMatched: insert} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedFail) {
    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "fail"
                                      << "whenNotMatched"
                                      << "insert"));
    ASSERT_THROWS_CODE_AND_WHAT(
        createMergeStage(std::move(spec)),
        DBException,
        ErrorCodes::StreamProcessorInvalidOptions,
        "StreamProcessorInvalidOptions: Unsupported whenMatched mode: fail");
}

// Test that {whenMatched: merge, on: [...]} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedMerge) {
    _context->expCtx->setMongoProcessInterface(
        std::make_shared<MongoProcessInterfaceForTest>(std::set<FieldPath>{"customerId"}));

    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "on" << BSON_ARRAY("customerId")));
    auto mergeOperator = createMergeStage(std::move(spec));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(
            Document(fromjson(fmt::format("{{_id: {}, a: {}, customerId: {}}}", i, i, 100 + i))));
        dataMsg.docs.emplace_back(
            Document(fromjson(fmt::format("{{_id: {}, b: {}, customerId: {}}}", i, i, 100 + i))));
    }
    dataMsg.creationTimer = mongo::Timer{};
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface = getMongoProcessInterface(mergeOperator.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    ASSERT_EQUALS(20, objsUpdated.size());
    for (int i = 0; i < 20; ++i) {
        ASSERT_EQUALS(MongoProcessInterface::UpsertType::kGenerateNewDoc, objsUpdated[i].upsert);
        ASSERT_FALSE(objsUpdated[i].multi);
        ASSERT_FALSE(objsUpdated[i].oid);
        const auto& batchObj = objsUpdated[i].batchObj;
        ASSERT_TRUE(std::get<0>(batchObj).hasField("customerId"));
        ASSERT_FALSE(std::get<2>(batchObj));
        auto& updateMod = std::get<1>(batchObj);
        auto updateObj = updateMod.getUpdateModifier()["$set"].Obj();
        ASSERT_BSONOBJ_EQ(updateObj, dataMsg.docs[i].doc.toBson());
    }

    mergeOperator->stop();
}

// Test that dead letter queue works as expected.
TEST_F(MergeOperatorTest, DeadLetterQueue) {
    _context->expCtx->setMongoProcessInterface(
        std::make_shared<MongoProcessInterfaceForTest>(std::set<FieldPath>{"customerId"}));

    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "on" << BSON_ARRAY("customerId")));
    auto mergeOperator = createMergeStage(std::move(spec), false /*allowMergeOnNullishValues*/);
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    // Create 3 documents, 2 with customerId field in them and 1 without.
    StreamDocument streamDoc(Document(fromjson("{_id: 0, a: 0, customerId: 100}")));
    StreamMetaSource streamMetaSource;
    streamMetaSource.setType(StreamMetaSourceTypeEnum::Kafka);
    streamMetaSource.setPartition(1);
    streamMetaSource.setOffset(10);
    streamDoc.streamMeta.setSource(streamMetaSource);
    dataMsg.docs.emplace_back(std::move(streamDoc));
    streamDoc = StreamDocument(Document(fromjson("{_id: 1, a: 1}")));
    streamMetaSource.setType(StreamMetaSourceTypeEnum::Kafka);
    streamMetaSource.setPartition(1);
    streamMetaSource.setOffset(20);
    streamDoc.streamMeta.setSource(streamMetaSource);
    dataMsg.docs.emplace_back(std::move(streamDoc));
    streamDoc = StreamDocument(Document(fromjson("{_id: 2, a: 2, customerId: 200}")));
    streamMetaSource.setType(StreamMetaSourceTypeEnum::Kafka);
    streamMetaSource.setPartition(1);
    streamMetaSource.setOffset(30);
    streamDoc.streamMeta.setSource(streamMetaSource);
    dataMsg.docs.emplace_back(std::move(streamDoc));
    dataMsg.creationTimer = mongo::Timer{};

    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface = getMongoProcessInterface(mergeOperator.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    ASSERT_EQUALS(2, objsUpdated.size());
    auto verifyObjUpdated = [&](size_t i, size_t j) {
        ASSERT_EQUALS(MongoProcessInterface::UpsertType::kGenerateNewDoc, objsUpdated[i].upsert);
        ASSERT_FALSE(objsUpdated[i].multi);
        ASSERT_FALSE(objsUpdated[i].oid);
        const auto& batchObj = objsUpdated[i].batchObj;
        ASSERT_TRUE(std::get<0>(batchObj).hasField("customerId"));
        ASSERT_FALSE(std::get<2>(batchObj));
        auto& updateMod = std::get<1>(batchObj);
        auto updateObj = updateMod.getUpdateModifier()["$set"].Obj();
        BSONObjBuilder expectedDocBuilder(dataMsg.docs[j].doc.toBson());
        expectedDocBuilder << "_stream_meta" << dataMsg.docs[j].streamMeta.toBSON();
        ASSERT_BSONOBJ_EQ(updateObj, expectedDocBuilder.obj());
    };
    verifyObjUpdated(0, 0);
    verifyObjUpdated(1, 2);

    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    ASSERT_EQ(1, dlqMsgs.size());
    ASSERT_EQ(mergeOperator->getStats().numDlqDocs, 1);
    ASSERT(mergeOperator->getStats().numDlqBytes > 0);
    auto dlqDoc = std::move(dlqMsgs.front());
    ASSERT_EQ(
        "Failed to process input document in MergeOperator with error: code = Location51132, "
        "reason = $merge write error: 'on' field 'customerId' cannot be missing, null or undefined "
        "if supporting index is sparse",
        dlqDoc["errInfo"]["reason"].String());
    ASSERT_BSONOBJ_EQ(dataMsg.docs[1].streamMeta.toBSON(), dlqDoc["_stream_meta"].Obj());
    ASSERT_EQ("MergeOperator", dlqDoc["operatorName"].String());

    mergeOperator->stop();
}

TEST_F(MergeOperatorTest, DocumentTooLarge) {
    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeOperator = createMergeStage(std::move(spec));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    const auto documentSize1MB = 1024 * 1024;
    MutableDocument mutDoc;
    for (int i = 0; i < 17; i++) {
        mutDoc.addField("value" + std::to_string(i), Value{std::string(documentSize1MB, 'a' + i)});
    }
    StreamDataMsg dataMsg{{mutDoc.freeze()}};
    dataMsg.creationTimer = mongo::Timer{};
    mergeOperator->onDataMsg(0, dataMsg);
    mergeOperator->flush();

    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    ASSERT_EQ(1, dlqMsgs.size());
    auto dlqDoc = std::move(dlqMsgs.front());
    ASSERT_STRING_CONTAINS(dlqDoc["errInfo"]["reason"].String(), "BSONObjectTooLarge");
    ASSERT_EQ("MergeOperator", dlqDoc["operatorName"].String());

    mergeOperator->stop();
}

TEST_F(MergeOperatorTest, DocumentBatchBoundary) {
    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeOperator = createMergeStage(std::move(spec));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    const auto documentSize4MB = 4 * 1024 * 1024;
    const auto documentSize3MB = 3 * 1024 * 1024;
    const auto documentSize8MB = 8 * 1024 * 1024;
    const auto documentSize12MB = 8 * 1024 * 1024;

    StreamDataMsg dataMsg;
    dataMsg.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize4MB, 'a')))));
    dataMsg.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize3MB, 'b')))));
    dataMsg.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize4MB, 'c')))));
    dataMsg.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize8MB, 'd')))));
    dataMsg.creationTimer = mongo::Timer{};

    mergeOperator->onDataMsg(0, dataMsg);
    mergeOperator->flush();

    auto processInterface = getMongoProcessInterface(mergeOperator.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    auto objsInserted = processInterface->getObjsInserted();
    ASSERT_EQ(objsUpdated.size(), 4);

    StreamDataMsg dataMsg2;
    dataMsg2.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize8MB, 'a')))));
    dataMsg2.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize4MB, 'b')))));
    dataMsg2.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize4MB, 'c')))));
    dataMsg2.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize8MB, 'd')))));
    dataMsg2.creationTimer = mongo::Timer{};

    mergeOperator->onDataMsg(0, dataMsg2);
    mergeOperator->flush();
    objsUpdated = processInterface->getObjsUpdated();
    objsInserted = processInterface->getObjsInserted();
    ASSERT_EQ(objsUpdated.size(), 8);

    StreamDataMsg dataMsg3;
    dataMsg3.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize8MB, 'a')))));
    dataMsg3.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize12MB, 'b')))));
    dataMsg3.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize3MB, 'c')))));
    dataMsg3.docs.emplace_back(
        Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(documentSize8MB, 'd')))));
    dataMsg3.creationTimer = mongo::Timer{};

    mergeOperator->onDataMsg(0, dataMsg3);
    mergeOperator->flush();
    objsUpdated = processInterface->getObjsUpdated();
    objsInserted = processInterface->getObjsInserted();
    ASSERT_EQ(objsUpdated.size(), 12);

    mergeOperator->stop();
}

// Test a batch that's larger than the maximum size.
TEST_F(MergeOperatorTest, BatchLargerThanQueueMaxSize) {
    // Copied from queued_sink_operator.cpp
    static constexpr int64_t kQueueMaxSizeBytes = 128 * 1024 * 1024;  // 128 MB
    const int64_t docSize = 10'000'000;
    const int maxDocsPerBatch = kQueueMaxSizeBytes / docSize;

    // Generate the large batch.
    auto generateDoc = [=]() {
        return Document(fromjson(fmt::format("{{value: \"{}\"}}", std::string(docSize, 'a'))));
    };
    StreamDataMsg dataMsg;
    const int docsInBatch = 2 * maxDocsPerBatch + 1;
    for (int i = 0; i < docsInBatch; ++i) {
        dataMsg.docs.push_back(generateDoc());
    }
    dataMsg.creationTimer = mongo::Timer{};

    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeOperator = createMergeStage(std::move(spec));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface = getMongoProcessInterface(mergeOperator.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    ASSERT_EQUALS(docsInBatch, objsUpdated.size());

    mergeOperator->stop();
}

// Tests basic correctness and partitioning when using $merge.parallelism >= 1.
TEST_F(MergeOperatorTest, Parallelism) {
    struct TestCase {
        std::vector<BSONObj> input;
        boost::optional<int> parallelism;
        size_t nextBatchSize{1};
        bool allThreadsShouldHaveData{false};
    };
    auto innerTest = [&, this](TestCase c) {
        std::cout << fmt::format("Running test case, parallelism: {}, input size: {}\n",
                                 c.parallelism ? std::to_string(*c.parallelism) : "none",
                                 c.input.size());

        _context->expCtx->setMongoProcessInterface(
            std::make_shared<MongoProcessInterfaceForTest>());
        _context->dlq->registerMetrics(_executor->getMetricManager());

        Connection atlasConn;
        atlasConn.setName("myconnection");
        AtlasConnectionOptions atlasConnOptions{"mongodb://localhost:27017"};
        atlasConn.setOptions(atlasConnOptions.toBSON());
        atlasConn.setType(ConnectionTypeEnum::Atlas);
        std::vector<mongo::Connection> connections{
            mongo::Connection{std::string("__testMemory"),
                              mongo::ConnectionTypeEnum::InMemory,
                              /* options */ mongo::BSONObj()}};
        connections.push_back(std::move(atlasConn));
        _context->connections = std::make_unique<ConnectionCollection>(connections);

        auto pipeline = R"([
            { "$source": {
                "connectionName": "__testMemory"
            }},
            { "$merge": {
                "into": {
                    "connectionName": "myconnection",
                    "db": "test",
                    "coll": "newColl2"
                },
                whenMatched: "replace"
            }}
        ]
        )";

        auto obj = fromjson(std::string{"{\"pipeline\": "} + pipeline + "}");
        auto rawPipeline = parsePipelineFromBSON(obj.getField("pipeline"));
        if (c.parallelism) {
            MutableDocument doc{Document(rawPipeline[1]["$merge"].Obj())};
            doc.setField("parallelism", Value(*c.parallelism));
            rawPipeline[1] = BSON("$merge" << doc.freeze().toBson());
        }

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        auto sink = dynamic_cast<QueuedSinkOperator*>(dag->operators().back().get());
        setForTest(sink);

        auto poll = [&](std::function<bool()> validate, std::string msg = "") {
            fmt::print("Polling: {}", msg);
            auto deadline = Date_t::now() + Seconds{10};
            while (Date_t::now() < deadline) {
                if (validate()) {
                    break;
                }
                sleepFor(Milliseconds(10));
            }
            ASSERT(validate());
        };

        Executor::Options executorOpts{.operatorDag = dag.get(),
                                       .checkpointCoordinator = nullptr,
                                       .metricManager = std::make_unique<MetricManager>()};

        Executor executor{_context.get(), std::move(executorOpts)};
        auto future = executor.start();
        poll([&]() { return executor.isConnected(); }, "waiting for connected");
        ScopeGuard stopper([&]() { executor.stop(StopReason::ExternalStopRequest, false); });

        // Validate the sink is using the expected number of writers.
        ASSERT_EQ(c.parallelism ? *c.parallelism : 1, getNumWriters(sink));

        std::vector<BSONObj> docs;
        for (const auto& obj : c.input) {
            docs.push_back(obj);
            if (docs.size() == c.nextBatchSize) {
                executor.testOnlyInsertDocuments(docs);
                docs.clear();
                c.nextBatchSize += 1;
            }
        }
        if (!docs.empty()) {
            executor.testOnlyInsertDocuments(docs);
        }

        // Wait for expected stats.
        poll(
            [&]() {
                auto executorStats = executor.getExecutorStats();
                auto stats = executorStats.operatorStats;
                if (stats.empty()) {
                    // this can happen before a runOnce has finished.
                    return false;
                }
                auto sinkStats = stats.back();
                return size_t(sinkStats.numInputDocs) == c.input.size() &&
                    size_t(sinkStats.numOutputDocs) == c.input.size();
            },
            "waiting for expected stats");

        // Validate expected results.
        auto docsEq = [&](const auto& input, const auto& results) {
            ASSERT_EQ(input.size(), results.size());

            auto sortedInput = input;
            auto sortedResults = results;
            auto sort = [](auto& container) {
                std::sort(container.begin(), container.end(), [](auto a, auto b) {
                    return SimpleBSONObjComparator::kInstance.evaluate(a < b);
                });
            };
            sort(sortedInput);
            sort(sortedResults);

            for (size_t i = 0; i < input.size(); ++i) {
                auto in = input[i];
                auto result = results[i];
                if (!in.hasField("_id")) {
                    MutableDocument doc{Document{result}};
                    doc.remove("_id");
                    result = doc.freeze().toBson();
                }

                if (!SimpleBSONObjComparator::kInstance.evaluate(in == result)) {
                    fmt::print("Did not find a match for {}\n", in.toString());
                }

                return true;
            }
            return true;
        };

        auto mongoInterfaces = getAllMongoProcessInterfaces(sink);
        std::vector<std::vector<BSONObj>> threadResults;
        std::vector<BSONObj> results;
        for (auto& mongo : mongoInterfaces) {
            auto threadUpdated = mongo->getObjsUpdated();
            threadResults.push_back({});
            for (auto result : threadUpdated) {
                auto obj = std::get<1>(result.batchObj).getUpdateReplacement();
                threadResults.back().push_back(obj);
                results.push_back(obj);
            }
        }
        ASSERT(docsEq(c.input, results));

        // Return a doc's partition-- an array of it's $merge.on fields.
        auto getPartition = [](const BSONObj& doc, const std::vector<std::string>& fieldNames) {
            std::vector<Value> mergeOnFields;
            mergeOnFields.reserve(fieldNames.size());
            for (const auto& name : fieldNames) {
                mergeOnFields.push_back(Value(doc.getField(name)));
            }

            return Value(mergeOnFields);
        };

        // Validate order in terms of $merge.on fields.
        using PartitionedData = ValueUnorderedMap<std::vector<BSONObj>>;
        std::vector<std::string> fieldNames = {"_id"};
        auto partition = [&](std::vector<BSONObj> data) {
            PartitionedData partitions(_context->expCtx->getValueComparator()
                                           .makeUnorderedValueMap<std::vector<BSONObj>>());
            for (auto& doc : data) {
                auto mergeOnFieldArray = getPartition(doc, fieldNames);
                if (!partitions.contains(mergeOnFieldArray)) {
                    partitions[mergeOnFieldArray] = {};
                }
                partitions[mergeOnFieldArray].push_back(doc);
            }
            return partitions;
        };
        auto partitionsEqual = [](PartitionedData l, PartitionedData r) {
            if (l.size() != r.size()) {
                return false;
            }
            for (auto& lPart : l) {
                auto it = r.find(lPart.first);
                if (it == r.end()) {
                    return false;
                }
                std::vector<BSONObj> rv = it->second;
                if (rv.size() != lPart.second.size()) {
                    return false;
                }
                for (size_t valueIdx = 0; valueIdx < lPart.second.size(); ++valueIdx) {
                    if (!SimpleBSONObjComparator::kInstance.evaluate(lPart.second[valueIdx] ==
                                                                     rv[valueIdx])) {
                        return false;
                    }
                }
            }
            return true;
        };

        bool allMergeOnFieldsSet{true};
        for (auto& doc : c.input) {
            for (auto& field : fieldNames) {
                if (!doc.hasField(field)) {
                    allMergeOnFieldsSet = false;
                    break;
                }
            }
        }
        if (allMergeOnFieldsSet) {
            ASSERT(partitionsEqual(partition(c.input), partition(results)));
        }

        if (c.allThreadsShouldHaveData) {
            // Validate that all threads have data.
            auto mongoInterfaces = getAllMongoProcessInterfaces(sink);
            std::vector<std::vector<BSONObj>> results;
            for (auto& mongo : mongoInterfaces) {
                results.push_back(std::vector<BSONObj>{});
                for (auto result : mongo->getObjsUpdated()) {
                    results.back().push_back(std::get<1>(result.batchObj).getUpdateReplacement());
                }
            }
            int parallelism = c.parallelism ? *c.parallelism : 1;
            ASSERT_EQ(std::ranges::count_if(
                          results, [](auto& threadResult) { return !threadResult.empty(); }),
                      parallelism);
        }

        // Validate all outputs for Value($merge.on fields) go only to one thread.
        using ThreadsForPartition = ValueUnorderedMap<std::set<int>>;
        ThreadsForPartition threadsForPartition(
            _context->expCtx->getValueComparator().makeUnorderedValueMap<std::set<int>>());
        for (size_t threadId = 0; threadId < threadResults.size(); ++threadId) {
            for (const auto& doc : threadResults[threadId]) {
                auto partition = getPartition(doc, fieldNames);
                if (!threadsForPartition.contains(partition)) {
                    threadsForPartition[partition] = {};
                }
                threadsForPartition[partition].insert(threadId);
                ASSERT_EQ(1, threadsForPartition[partition].size());
            }
        }
    };

    for (auto parallelNum = 0; parallelNum < 16; ++parallelNum) {
        boost::optional<int> parallelism;
        if (parallelNum > 0) {
            parallelism = parallelNum;
        }

        auto baseObj = [](int i) {
            return BSON("i" << i << "a"
                            << "The quick brown fox");
        };
        using ModifyObjectFunc = std::function<void(BSONObjBuilder&, int)>;
        auto makeInput = [&](int size, ModifyObjectFunc modifyObj) {
            std::vector<BSONObj> result;
            result.reserve(size);
            for (int i = 0; i < size; ++i) {
                BSONObjBuilder b(baseObj(i));
                modifyObj(b, i);
                result.push_back(b.obj());
            }
            return result;
        };

        std::vector<ModifyObjectFunc> inputFuncs{
            [&](BSONObjBuilder& builder, int i) {},
            // id set to uniqe value
            [&](BSONObjBuilder& builder, int i) { builder.append("_id", i); },
            // sometimes id set
            [&](BSONObjBuilder& builder, int i) {
                if (bool(0x1 & i)) {
                    builder.append("_id", i);
                }
            },
            // sometimes id set
            [&](BSONObjBuilder& builder, int i) {
                if (bool(0x1 & i)) {
                    builder.append("_id", i);
                }
            },
            // many matching IDs
            [&](BSONObjBuilder& builder, int i) { builder.append("_id", i % 5); },
            // constant ID
            [&](BSONObjBuilder& builder, int i) { builder.append("_id", "foo"); },
        };
        for (size_t idx = 0; idx < inputFuncs.size(); ++idx) {
            const auto& inputFunc = inputFuncs[idx];
            for (auto size : std::vector<int>{1, 2, 1024, 1234, 4092, 9993}) {
                bool allThreadsShouldHaveData = size > 4000 && (idx == 0 || idx == 1);
                innerTest(TestCase{.input = makeInput(size, inputFunc),
                                   .parallelism = parallelism,
                                   .allThreadsShouldHaveData = allThreadsShouldHaveData});
            }
        }
    }
}

// Executes $merge using a collection in local MongoDB deployment.
TEST_F(MergeOperatorTest, LocalTest) {
    // Sample value for the envvar: mongodb://localhost:27017
    if (!std::getenv("MERGE_TEST_MONGODB_URI")) {
        return;
    }

    _context->expCtx->getMongoProcessInterface().reset();

    Connection atlasConn;
    atlasConn.setName("myconnection");
    AtlasConnectionOptions atlasConnOptions{std::getenv("MERGE_TEST_MONGODB_URI")};
    atlasConn.setOptions(atlasConnOptions.toBSON());
    atlasConn.setType(ConnectionTypeEnum::Atlas);
    auto connectionsVector = testInMemoryConnections();
    connectionsVector.push_back(atlasConn);
    _context->connections = std::make_unique<ConnectionCollection>(connectionsVector);

    NamespaceString fromNs =
        NamespaceString::createNamespaceString_forTest(boost::none, "test", "foreign_coll");
    _context->expCtx->setResolvedNamespaces(
        ResolvedNamespaceMap{{fromNs, {fromNs, std::vector<BSONObj>()}}});

    auto mergeObj = fromjson(R"(
{
  $merge: {
    into: {
      connectionName: "myconnection",
      db: "test",
      coll: "newColl2"
    },
    on: ["x", "y"]
  }
})");

    std::vector<BSONObj> rawPipeline{getTestSourceSpec(), mergeObj};

    Planner planner(_context.get(), /*options*/ {});
    auto dag = planner.plan(rawPipeline);
    dag->start();
    auto source = dynamic_cast<InMemorySourceOperator*>(dag->operators().front().get());
    auto sink = dynamic_cast<QueuedSinkOperator*>(dag->operators().back().get());

    std::vector<BSONObj> inputDocs;
    std::vector<int> vals = {5, 2};
    inputDocs.reserve(vals.size());
    for (auto& val : vals) {
        inputDocs.emplace_back(fromjson(fmt::format("{{x: {}, y: {}}}", val, val + 1)));
    }

    StreamDataMsg dataMsg;
    for (auto& inputDoc : inputDocs) {
        dataMsg.docs.emplace_back(Document(inputDoc));
    }
    source->addDataMsg(std::move(dataMsg));
    source->runOnce();
    sink->flush();
    dag->stop();

    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    auto dlqMsgs = dlq->getMessages();
    while (!dlqMsgs.empty()) {
        std::cout << "dlqDoc: " << dlqMsgs.front() << std::endl;
        dlqMsgs.pop();
    }
}

// Test partitioning logic
TEST_F(MergeOperatorTest, Partitioning) {
    auto makeOp = [&](std::string spec) {
        _context->expCtx->setMongoProcessInterface(
            std::make_shared<MongoProcessInterfaceForTest>());
        _context->dlq->registerMetrics(_executor->getMetricManager());

        Connection atlasConn;
        atlasConn.setName("myconnection");
        AtlasConnectionOptions atlasConnOptions{"mongodb://localhost:27017"};
        atlasConn.setOptions(atlasConnOptions.toBSON());
        atlasConn.setType(ConnectionTypeEnum::Atlas);
        std::vector<mongo::Connection> connections{
            mongo::Connection{std::string("__testMemory"),
                              mongo::ConnectionTypeEnum::InMemory,
                              /* options */ mongo::BSONObj()}};
        connections.push_back(std::move(atlasConn));
        _context->connections = std::make_unique<ConnectionCollection>(connections);

        std::vector<BSONObj> rawPipeline{BSON("$source" << BSON("connectionName"
                                                                << "__testMemory")),
                                         fromjson(spec)};

        Planner planner(_context.get(), /*options*/ {});
        auto dag = planner.plan(rawPipeline);
        auto sink = dynamic_cast<MergeOperator*>(dag->operators().back().get());
        setForTest(sink);
        sink->registerMetrics(_executor->getMetricManager());
        start(sink);
        return std::make_tuple(std::move(dag), sink, getWriter(sink));
    };

    // literal case
    std::unique_ptr<OperatorDag> dag;
    MergeOperator* op{nullptr};
    SinkWriter* writer{nullptr};
    std::tie(dag, op, writer) = makeOp(R"({ "$merge": {
        "into": {
            "connectionName": "myconnection",
            "db": "test",
            "coll": "newColl2"
        },
        whenMatched: "replace"
    }})");

    auto eq = [&](auto one, auto two) {
        StreamDocument doc1{Document(one)};
        StreamDocument doc2{Document(two)};
        fmt::print("eq: {} {}\n", doc1.doc.toString(), doc2.doc.toString());
        auto result1 = writer->partition(doc1);
        auto result2 = writer->partition(doc2);
        ASSERT(result1.isOK());
        ASSERT(result2.isOK());
        ASSERT_EQ(result2.getValue(), result1.getValue());
    };
    auto ne = [&](auto one, auto two) {
        StreamDocument doc1{Document(one)};
        StreamDocument doc2{Document(two)};
        fmt::print("ne: {} {}\n", doc1.doc.toString(), doc2.doc.toString());
        auto result1 = writer->partition(doc1);
        auto result2 = writer->partition(doc2);
        ASSERT(result1.isOK());
        ASSERT(result2.isOK());
        ASSERT_NE(result2.getValue(), result1.getValue());
    };
    auto err = [&](auto doc, const std::string& errMsg) {
        StreamDocument doc1{Document(doc)};
        fmt::print("err: {}\n", doc1.doc.toString());
        auto result = writer->partition(doc1);
        fmt::print("actual error: {}\n", result.getStatus().reason());
        ASSERT(!result.isOK());
        ASSERT(result.getStatus().reason().find(errMsg) != std::string::npos);
    };

    // Verify the partition method adds _id.
    StreamDocument doc(Document(BSON("a" << 1)));
    ASSERT(writer->partition(doc).isOK());
    ASSERT(!doc.doc["_id"].missing());
    doc = (Document(BSON("a" << 2)));
    ASSERT(writer->partition(doc).isOK());
    ASSERT(!doc.doc["_id"].missing());

    // Verify same _id leads to same partition.
    eq(Document(BSON("a" << 2 << "_id" << 2)), Document(BSON("a" << 3 << "_id" << 2)));
    eq(Document(BSON("_id" << -2)), Document(BSON("_id" << -2)));

    // Different these different _id lead to different partition.
    ne(Document(BSON("_id" << 2)), Document(BSON("_id" << 3)));
    ne(Document(BSON("_id" << 2)), Document(BSON("_id" << -2)));

    // literal case with custom on fields
    op->stop();
    std::tie(dag, op, writer) = makeOp(R"({ "$merge": {
        "into": {
            "connectionName": "myconnection",
            "db": "test",
            "coll": "newColl2"
        },
        on: ["foo", "bar"],
        whenMatched: "replace"
    }})");
    // no on fields, _id specified
    eq(Document(BSON("_id" << 2)), Document(BSON("_id" << 2)));
    // no on fields, _id not specified
    eq(Document(BSON("a" << 2)), Document(BSON("a" << 3)));
    // no on fields, _id not specified, different non-on fields
    eq(Document(BSON("a" << 2)), Document(BSON("a" << 2)));
    // id specified
    auto obj = BSON("foo" << 3 << "bar" << 4);
    eq(obj, obj);
    eq(obj, Document(BSONObjBuilder(obj).append("irrelevant", "field").obj()));
    ne(BSON("foo" << 3 << "bar" << 5), obj);
    ne(BSON("foo" << 4 << "bar" << 4), obj);
    ne(BSON("foo" << 2 << "bar" << 3), obj);

    // into.coll is an expression
    op->stop();
    std::tie(dag, op, writer) = makeOp(R"({ "$merge": {
        "into": {
            "connectionName": "myconnection",
            "db": "test",
            "coll": "$coll"
        }
    }})");
    auto justColl1 = BSON("coll"
                          << "one");
    auto justColl2 = BSON("coll"
                          << "two");

    // id gets generated but coll is the same
    ne(justColl1, justColl1);
    // different id, same coll
    ne(BSON("coll"
            << "one"
            << "_id" << 2),
       BSON("coll"
            << "one"
            << "_id" << 3));
    // same id, same coll
    eq(BSON("coll"
            << "one"
            << "_id" << 2),
       BSON("coll"
            << "one"
            << "_id" << 2));

    // evaluates to null
    err(BSON("coll" << BSONNULL << "_id" << 2),
        "Failed to evaluate target namespace in MergeOperator :: caused by :: Expected string, but "
        "got null");

    // expression failure
    op->stop();
    std::tie(dag, op, writer) = makeOp(R"({ "$merge": {
        "into": {
            "connectionName": "myconnection",
            "db": "test",
            "coll": {"$divide": ["$one", "$two"]}
        }
    }})");
    err(BSON("one" << 1 << "two" << 0),
        "Failed to evaluate target namespace in MergeOperator :: caused by :: can't $divide by "
        "zero");

    op->stop();
}

}  // namespace streams
