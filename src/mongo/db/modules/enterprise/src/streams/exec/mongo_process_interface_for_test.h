#pragma once

/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <fmt/format.h>
#include <memory>

#include "mongo/bson/json.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/aggregation_request_helper.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/planner.h"

namespace streams {

/**
 * MongoProcessInterfaceForTest is used in MergeOperator unit tests. It's used to mock a MongoDB
 * target.
 */
class MongoProcessInterfaceForTest : public MongoDBProcessInterface {
public:
    MongoProcessInterfaceForTest(
        std::set<mongo::FieldPath> documentKey = {"_id"},
        MongoProcessInterface::SupportingUniqueIndex supportingUniqueIndex =
            MongoProcessInterface::SupportingUniqueIndex::NotNullish)
        : _documentKey(std::move(documentKey)), _supportingUniqueIndex(supportingUniqueIndex) {}

    struct InsertInfo {
        mongo::BSONObj obj;
        boost::optional<mongo::OID> oid;
    };

    struct UpdateInfo {
        MongoProcessInterface::BatchObject batchObj;
        UpsertType upsert;
        bool multi;
        boost::optional<mongo::OID> oid;
    };

    class WriteSizeEstimatorForTest final : public WriteSizeEstimator {
    public:
        int estimateInsertHeaderSize(
            const mongo::write_ops::InsertCommandRequest& insertReq) const override {
            return 0;
        }
        int estimateUpdateHeaderSize(
            const mongo::write_ops::UpdateCommandRequest& insertReq) const override {
            return 0;
        }

        int estimateInsertSizeBytes(const mongo::BSONObj& insert) const override {
            return 0;
        }

        int estimateUpdateSizeBytes(const BatchObject& batchObject,
                                    UpsertType type) const override {
            return 0;
        }
    };

    std::unique_ptr<WriteSizeEstimator> getWriteSizeEstimator(
        mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) const override {
        return std::make_unique<WriteSizeEstimatorForTest>();
    }

    bool isSharded(mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) override {
        return false;
    }

    boost::optional<mongo::ShardId> determineSpecificMergeShard(
        mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) const override {
        return boost::none;
    }

    std::vector<mongo::FieldPath> collectDocumentKeyFieldsActingAsRouter(
        mongo::OperationContext* opCtx, const mongo::NamespaceString& nss) const override {
        return {"_id"};
    }

    void checkRoutingInfoEpochOrThrow(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                      const mongo::NamespaceString&,
                                      mongo::ChunkVersion) const override {
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

    mongo::Status insert(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                         const mongo::NamespaceString& ns,
                         std::unique_ptr<mongo::write_ops::InsertCommandRequest> insertReq,
                         const mongo::WriteConcernOptions& wc,
                         boost::optional<mongo::OID> oid) override {
        mongo::stdx::lock_guard<mongo::stdx::mutex> lock(_mutex);
        for (auto& obj : insertReq->getDocuments()) {
            InsertInfo iInfo;
            iInfo.obj = std::move(obj);
            iInfo.oid = oid;
            _objsInserted.push_back(iInfo);
        }
        return mongo::Status::OK();
    }

    mongo::StatusWith<UpdateResult> update(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& ns,
        std::unique_ptr<mongo::write_ops::UpdateCommandRequest> updateReq,
        const mongo::WriteConcernOptions& wc,
        UpsertType upsert,
        bool multi,
        boost::optional<mongo::OID> oid) override {
        mongo::stdx::lock_guard<mongo::stdx::mutex> lock(_mutex);
        for (auto& updateOp : updateReq->getUpdates()) {
            UpdateInfo uInfo;
            uInfo.batchObj = MongoProcessInterface::BatchObject{
                updateOp.getQ(), updateOp.getU(), updateOp.getC()};
            uInfo.upsert = upsert;
            uInfo.multi = multi;
            uInfo.oid = oid;
            _objsUpdated.push_back(uInfo);
        }
        return mongo::StatusWith(UpdateResult{});
    }

    const std::vector<InsertInfo>& getObjsInserted() const {
        mongo::stdx::lock_guard<mongo::stdx::mutex> lock(_mutex);
        return _objsInserted;
    }

    const std::vector<UpdateInfo>& getObjsUpdated() const {
        mongo::stdx::lock_guard<mongo::stdx::mutex> lock(_mutex);
        return _objsUpdated;
    }

    void testConnection(const mongo::NamespaceString& nss) override {}

    const mongocxx::uri& uri() override {
        return _uri;
    }

private:
    mongocxx::uri _uri{"mongodb://localhost:27017"};
    std::set<mongo::FieldPath> _documentKey;
    MongoProcessInterface::SupportingUniqueIndex _supportingUniqueIndex;

    std::vector<InsertInfo> _objsInserted;
    std::vector<UpdateInfo> _objsUpdated;

    mutable mongo::stdx::mutex _mutex;
};

}  // namespace streams
