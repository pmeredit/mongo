/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>
#include <memory>

#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/in_memory_source_operator.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include "streams/exec/queued_sink_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"
namespace streams {

using namespace mongo;
using namespace std::string_literals;

// Override StubMongoProcessInterface like done in document_source_merge_test.cpp
class MongoProcessInterfaceForTest : public MongoDBProcessInterface {
public:
    MongoProcessInterfaceForTest(
        std::set<FieldPath> documentKey = {"_id"},
        MongoProcessInterface::SupportingUniqueIndex supportingUniqueIndex =
            MongoProcessInterface::SupportingUniqueIndex::NotNullish)
        : _documentKey(std::move(documentKey)), _supportingUniqueIndex(supportingUniqueIndex) {}

    struct InsertInfo {
        BSONObj obj;
        boost::optional<OID> oid;
    };

    struct UpdateInfo {
        MongoProcessInterface::BatchObject batchObj;
        UpsertType upsert;
        bool multi;
        boost::optional<OID> oid;
    };

    class WriteSizeEstimatorForTest final : public WriteSizeEstimator {
    public:
        int estimateInsertHeaderSize(
            const write_ops::InsertCommandRequest& insertReq) const override {
            return 0;
        }
        int estimateUpdateHeaderSize(
            const write_ops::UpdateCommandRequest& insertReq) const override {
            return 0;
        }

        int estimateInsertSizeBytes(const BSONObj& insert) const override {
            return 0;
        }

        int estimateUpdateSizeBytes(const BatchObject& batchObject,
                                    UpsertType type) const override {
            return 0;
        }
    };

    std::unique_ptr<WriteSizeEstimator> getWriteSizeEstimator(
        OperationContext* opCtx, const NamespaceString& ns) const override {
        return std::make_unique<WriteSizeEstimatorForTest>();
    }

    bool isSharded(OperationContext* opCtx, const NamespaceString& ns) override {
        return false;
    }

    boost::optional<ShardId> determineSpecificMergeShard(OperationContext* opCtx,
                                                         const NamespaceString& ns) const override {
        return boost::none;
    }

    std::vector<FieldPath> collectDocumentKeyFieldsActingAsRouter(
        OperationContext* opCtx, const NamespaceString& nss) const override {
        return {"_id"};
    }

    void checkRoutingInfoEpochOrThrow(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                      const NamespaceString&,
                                      ChunkVersion) const override {
        return;
    }

    void initConfigCollection() override {}

    void ensureCollectionExists(const mongo::NamespaceString& ns) override {}

    DocumentKeyResolutionMetadata ensureFieldsUniqueOrResolveDocumentKey(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        boost::optional<std::set<mongo::FieldPath>> fieldPaths,
        boost::optional<mongo::ChunkVersion> targetCollectionPlacementVersion,
        const mongo::NamespaceString& outputNs) const override {
        return {_documentKey, boost::none, _supportingUniqueIndex};
    }

    Status insert(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                  const NamespaceString& ns,
                  std::unique_ptr<write_ops::InsertCommandRequest> insertReq,
                  const WriteConcernOptions& wc,
                  boost::optional<OID> oid) override {
        for (auto& obj : insertReq->getDocuments()) {
            InsertInfo iInfo;
            iInfo.obj = std::move(obj);
            iInfo.oid = oid;
            _objsInserted.push_back(iInfo);
        }
        return Status::OK();
    }

    StatusWith<UpdateResult> update(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                    const NamespaceString& ns,
                                    std::unique_ptr<write_ops::UpdateCommandRequest> updateReq,
                                    const WriteConcernOptions& wc,
                                    UpsertType upsert,
                                    bool multi,
                                    boost::optional<OID> oid) override {
        for (auto& updateOp : updateReq->getUpdates()) {
            UpdateInfo uInfo;
            uInfo.batchObj = MongoProcessInterface::BatchObject{
                updateOp.getQ(), updateOp.getU(), updateOp.getC()};
            uInfo.upsert = upsert;
            uInfo.multi = multi;
            uInfo.oid = oid;
            _objsUpdated.push_back(uInfo);
        }
        return StatusWith(UpdateResult{});
    }

    const std::vector<InsertInfo>& getObjsInserted() const {
        return _objsInserted;
    }

    const std::vector<UpdateInfo>& getObjsUpdated() const {
        return _objsUpdated;
    }

    void testConnection(const mongo::NamespaceString& nss) override {}

    const mongocxx::uri& uri() override {
        return _uri;
    }

private:
    mongocxx::uri _uri{"mongodb://localhost:27017"};
    std::set<FieldPath> _documentKey;
    MongoProcessInterface::SupportingUniqueIndex _supportingUniqueIndex;

    std::vector<InsertInfo> _objsInserted;
    std::vector<UpdateInfo> _objsUpdated;
};

class MergeOperatorTest : public AggregationContextFixture {
public:
    MergeOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
    }

    void setUp() override {
        std::tie(_context, _executor) = getTestContext(/*svcCtx*/ nullptr);
        _context->expCtx->mongoProcessInterface = std::make_shared<MongoProcessInterfaceForTest>();
        _context->dlq->registerMetrics(_executor->getMetricManager());
    }

    // Start the MergeOperator and wait for it to connect.
    void start(MergeOperator* op) {
        op->start();
        auto deadline = Date_t::now() + Seconds{10};
        // Wait for the source to be connected like the Executor does.
        while (op->getConnectionStatus().isConnecting()) {
            stdx::this_thread::sleep_for(stdx::chrono::milliseconds(100));
            ASSERT(Date_t::now() < deadline);
        }
        ASSERT(op->getConnectionStatus().isConnected());
    }

    boost::intrusive_ptr<DocumentSourceMerge> createMergeStage(BSONObj spec) {
        auto specElem = spec.firstElement();
        boost::intrusive_ptr<DocumentSourceMerge> mergeStage = dynamic_cast<DocumentSourceMerge*>(
            DocumentSourceMerge::createFromBson(specElem, _context->expCtx).get());
        ASSERT_TRUE(mergeStage);
        return mergeStage;
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
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    // Arbitrary names for db and coll work fine since we use a mock MongoProcessInterface.
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }
    dataMsg.creationTimer = mongo::Timer{};

    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
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
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    // Arbitrary names for db and coll work fine since we use a mock MongoProcessInterface.
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }
    dataMsg.creationTimer = mongo::Timer{};
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
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
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    // Arbitrary names for db and coll work fine since we use a mock MongoProcessInterface.
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }
    dataMsg.creationTimer = mongo::Timer{};
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
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
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    // Arbitrary names for db and coll work fine since we use a mock MongoProcessInterface.
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }
    dataMsg.creationTimer = mongo::Timer{};
    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
    auto objsInserted = processInterface->getObjsInserted();
    ASSERT_EQUALS(10, objsInserted.size());
    for (int i = 0; i < 10; ++i) {
        ASSERT_FALSE(objsInserted[i].oid);
        ASSERT_TRUE(objsInserted[i].obj.hasField("_id"));
        ASSERT_BSONOBJ_EQ(objsInserted[i].obj, dataMsg.docs[i].doc.toBson());
    }

    mergeOperator->stop();
}

// Test that {whenMatched: merge, on: [...]} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedMerge) {
    _context->expCtx->mongoProcessInterface =
        std::make_shared<MongoProcessInterfaceForTest>(std::set<FieldPath>{"customerId"});

    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "on" << BSON_ARRAY("customerId")));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    // Arbitrary names for db and coll work fine since we use a mock MongoProcessInterface.
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
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

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
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
    _context->expCtx->mongoProcessInterface =
        std::make_shared<MongoProcessInterfaceForTest>(std::set<FieldPath>{"customerId"});

    // The 'into' field is not used and just a placeholder.
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "on" << BSON_ARRAY("customerId")));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    // Arbitrary names for db and coll work fine since we use a mock MongoProcessInterface.
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
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

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
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
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
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
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
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

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
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
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    // Arbitrary names for db and coll work fine since we use a mock MongoProcessInterface.
    MergeOperator::Options options{.documentSource = mergeStage.get(),
                                   .db = "test"s,
                                   .coll = "coll"s,
                                   .mergeExpCtx = _context->expCtx};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->registerMetrics(_executor->getMetricManager());
    start(mergeOperator.get());

    mergeOperator->onDataMsg(0, dataMsg, boost::none);
    mergeOperator->flush();

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
    auto objsUpdated = processInterface->getObjsUpdated();
    ASSERT_EQUALS(docsInBatch, objsUpdated.size());

    mergeOperator->stop();
}

// Executes $merge using a collection in local MongoDB deployment.
TEST_F(MergeOperatorTest, LocalTest) {
    // Sample value for the envvar: mongodb://localhost:27017
    if (!std::getenv("MERGE_TEST_MONGODB_URI")) {
        return;
    }

    _context->expCtx->mongoProcessInterface.reset();

    Connection atlasConn;
    atlasConn.setName("myconnection");
    AtlasConnectionOptions atlasConnOptions{std::getenv("MERGE_TEST_MONGODB_URI")};
    atlasConn.setOptions(atlasConnOptions.toBSON());
    atlasConn.setType(ConnectionTypeEnum::Atlas);
    _context->connections = testInMemoryConnectionRegistry();
    _context->connections.insert(std::make_pair(atlasConn.getName().toString(), atlasConn));

    NamespaceString fromNs =
        NamespaceString::createNamespaceString_forTest(boost::none, "test", "foreign_coll");
    _context->expCtx->setResolvedNamespaces(
        StringMap<ResolvedNamespace>{{fromNs.coll().toString(), {fromNs, std::vector<BSONObj>()}}});

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
    auto sink = dynamic_cast<MergeOperator*>(dag->operators().back().get());

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

}  // namespace streams
