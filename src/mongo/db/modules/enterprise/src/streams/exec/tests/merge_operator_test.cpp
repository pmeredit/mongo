/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/merge_operator.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/util/metric_manager.h"

namespace streams {
namespace {

using namespace mongo;

// Override StubMongoProcessInterface like done in document_source_merge_test.cpp
class MongoProcessInterfaceForTest : public StubMongoProcessInterface {
public:
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

    std::vector<FieldPath> collectDocumentKeyFieldsActingAsRouter(
        OperationContext* opCtx, const NamespaceString& nss) const override {
        return {"_id"};
    }

    void checkRoutingInfoEpochOrThrow(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                      const NamespaceString&,
                                      ChunkVersion) const override {
        return;
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

private:
    std::vector<InsertInfo> _objsInserted;
    std::vector<UpdateInfo> _objsUpdated;
};

class MergeOperatorTest : public AggregationContextFixture {
public:
    MergeOperatorTest() : AggregationContextFixture() {
        _metricManager = std::make_unique<MetricManager>();
        _context = getTestContext(/*svcCtx*/ nullptr, _metricManager.get());
        _context->expCtx->mongoProcessInterface = std::make_shared<MongoProcessInterfaceForTest>();
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
};

// Test that {whenMatched: replace, whenNotMatched: insert} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedReplace) {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get()};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

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
}

// Test that {whenMatched: replace, whenNotMatched: discard} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedReplaceDiscard) {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "replace"
                                      << "whenNotMatched"
                                      << "discard"));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get()};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

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
}

// Test that {whenMatched: keepExisting, whenNotMatched: insert} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedKeepExisting) {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "keepExisting"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get()};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

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
}

// Test that {whenMatched: fail, whenNotMatched: insert} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedFail) {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "fail"
                                      << "whenNotMatched"
                                      << "insert"));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get()};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(_context->expCtx->mongoProcessInterface.get());
    auto objsInserted = processInterface->getObjsInserted();
    ASSERT_EQUALS(10, objsInserted.size());
    for (int i = 0; i < 10; ++i) {
        ASSERT_FALSE(objsInserted[i].oid);
        ASSERT_TRUE(objsInserted[i].obj.hasField("_id"));
        ASSERT_BSONOBJ_EQ(objsInserted[i].obj, dataMsg.docs[i].doc.toBson());
    }
}

// Test that {whenMatched: merge, on: [...]} works as expected.
TEST_F(MergeOperatorTest, WhenMatchedMerge) {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "on" << BSON_ARRAY("customerId")));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get()};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(
            Document(fromjson(fmt::format("{{_id: {}, a: {}, customerId: {}}}", i, i, 100 + i))));
        dataMsg.docs.emplace_back(
            Document(fromjson(fmt::format("{{_id: {}, b: {}, customerId: {}}}", i, i, 100 + i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

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
}

// Test that dead letter queue works as expected.
TEST_F(MergeOperatorTest, DeadLetterQueue) {
    auto spec = BSON("$merge" << BSON("into"
                                      << "target_collection"
                                      << "whenMatched"
                                      << "merge"
                                      << "on" << BSON_ARRAY("customerId")));
    auto mergeStage = createMergeStage(std::move(spec));
    ASSERT(mergeStage);

    MergeOperator::Options options{.documentSource = mergeStage.get()};
    auto mergeOperator = std::make_unique<MergeOperator>(_context.get(), std::move(options));
    mergeOperator->start();

    StreamDataMsg dataMsg;
    // Create 3 documents, 2 with customerId field in them and 1 without.
    StreamDocument streamDoc(Document(fromjson("{_id: 0, a: 0, customerId: 100}")));
    streamDoc.streamMeta.setSourceType(StreamMetaSourceTypeEnum::Kafka);
    streamDoc.streamMeta.setSourcePartition(1);
    streamDoc.streamMeta.setSourceOffset(10);
    dataMsg.docs.emplace_back(std::move(streamDoc));
    streamDoc = StreamDocument(Document(fromjson("{_id: 1, a: 1}")));
    streamDoc.streamMeta.setSourceType(StreamMetaSourceTypeEnum::Kafka);
    streamDoc.streamMeta.setSourcePartition(1);
    streamDoc.streamMeta.setSourceOffset(20);
    dataMsg.docs.emplace_back(std::move(streamDoc));
    streamDoc = StreamDocument(Document(fromjson("{_id: 2, a: 2, customerId: 200}")));
    streamDoc.streamMeta.setSourceType(StreamMetaSourceTypeEnum::Kafka);
    streamDoc.streamMeta.setSourcePartition(1);
    streamDoc.streamMeta.setSourceOffset(30);
    dataMsg.docs.emplace_back(std::move(streamDoc));

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

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
    auto dlqDoc = std::move(dlqMsgs.front());
    ASSERT_EQ(
        "Failed to process input document in MergeOperator with error: $merge write error: "
        "'on' field 'customerId' cannot be missing, null, undefined or an array",
        dlqDoc["errInfo"]["reason"].String());
    ASSERT_BSONOBJ_EQ(dataMsg.docs[1].streamMeta.toBSON(), dlqDoc["_stream_meta"].Obj());
}

}  // namespace
}  // namespace streams
