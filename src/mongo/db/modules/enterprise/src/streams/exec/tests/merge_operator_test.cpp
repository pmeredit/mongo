/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <fmt/format.h>

#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/unittest/unittest.h"

#include "streams/exec/merge_operator.h"

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
        getExpCtx()->mongoProcessInterface = std::make_shared<MongoProcessInterfaceForTest>();
    }

    boost::intrusive_ptr<DocumentSourceMerge> createMergeStage(BSONObj spec) {
        auto specElem = spec.firstElement();
        boost::intrusive_ptr<DocumentSourceMerge> mergeStage = dynamic_cast<DocumentSourceMerge*>(
            DocumentSourceMerge::createFromBson(specElem, getExpCtx()).get());
        ASSERT_TRUE(mergeStage);
        return mergeStage;
    }
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

    auto mergeOperator = std::make_unique<MergeOperator>(mergeStage.get());
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(getExpCtx()->mongoProcessInterface.get());
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

    auto mergeOperator = std::make_unique<MergeOperator>(mergeStage.get());
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(getExpCtx()->mongoProcessInterface.get());
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

    auto mergeOperator = std::make_unique<MergeOperator>(mergeStage.get());
    mergeOperator->start();

    StreamDataMsg dataMsg;
    for (int i = 0; i < 10; ++i) {
        dataMsg.docs.emplace_back(Document(fromjson(fmt::format("{{_id: {}, a: {}}}", i, i))));
    }

    mergeOperator->onDataMsg(0, dataMsg, boost::none);

    auto processInterface =
        dynamic_cast<MongoProcessInterfaceForTest*>(getExpCtx()->mongoProcessInterface.get());
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

    auto mergeOperator = std::make_unique<MergeOperator>(mergeStage.get());
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
        dynamic_cast<MongoProcessInterfaceForTest*>(getExpCtx()->mongoProcessInterface.get());
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

}  // namespace
}  // namespace streams
