/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <queue>

#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/process_interface/mongo_process_interface.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/exec/mongocxx_utils.h"

namespace streams {

// An implementation of MongoProcessInterface that writes to / reads from the specified MongoDB
// instance.
class MongoDBProcessInterface : public mongo::MongoProcessInterface {
public:
    MongoDBProcessInterface(const streams::MongoCxxClientOptions& options);

    // Test-only constructor.
    MongoDBProcessInterface();

    std::unique_ptr<mongo::TransactionHistoryIteratorBase> createTransactionHistoryIterator(
        mongo::repl::OpTime time) const override {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<WriteSizeEstimator> getWriteSizeEstimator(
        mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) const override;

    bool isSharded(mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) override {
        // TODO(SERVER-77198): Fix this.
        return false;
    }

    boost::optional<mongo::ShardId> determineSpecificMergeShard(
        mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) const override {
        return boost::none;
    }

    void updateClientOperationTime(mongo::OperationContext* opCtx) const override {
        // We don't need to support causal consistency, so do nothing.
    }

    mongo::Status insert(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                         const mongo::NamespaceString& ns,
                         std::unique_ptr<mongo::write_ops::InsertCommandRequest> insertCommand,
                         const mongo::WriteConcernOptions& wc,
                         boost::optional<mongo::OID> oid) override;

    mongo::StatusWith<UpdateResult> update(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& ns,
        std::unique_ptr<mongo::write_ops::UpdateCommandRequest> updateCommand,
        const mongo::WriteConcernOptions& wc,
        UpsertType upsert,
        bool multi,
        boost::optional<mongo::OID> oid) override;

    template <typename T>
    mongo::BSONObj runCommand(const T& request) {
        auto db = getDb(request.getDbName());
        // Lets the operation_exception be thrown if the operation fails.
        auto cmdObj = request.toBSON();
        auto reply = db->run_command({toBsoncxxValue(std::move(cmdObj))});
        return fromBsoncxxDocument(reply);
    }

    mongo::Status insertTimeseries(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& ns,
        std::unique_ptr<mongo::write_ops::InsertCommandRequest> insertCommand,
        const mongo::WriteConcernOptions& wc,
        boost::optional<mongo::OID> targetEpoch) override {
        MONGO_UNREACHABLE;
    }

    std::vector<mongo::Document> getIndexStats(mongo::OperationContext* opCtx,
                                               const mongo::NamespaceString& ns,
                                               mongo::StringData host,
                                               bool addShardName) override {
        MONGO_UNREACHABLE;
    }

    std::list<mongo::BSONObj> getIndexSpecs(mongo::OperationContext* opCtx,
                                            const mongo::NamespaceString& ns,
                                            bool includeBuildUUIDs) override {
        MONGO_UNREACHABLE;
    }

    std::deque<mongo::BSONObj> listCatalog(mongo::OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    void createTimeseriesView(mongo::OperationContext* opCtx,
                              const mongo::NamespaceString& ns,
                              const mongo::BSONObj& cmdObj,
                              const mongo::TimeseriesOptions& userOpts) final {
        MONGO_UNREACHABLE;
    }

    boost::optional<mongo::BSONObj> getCatalogEntry(
        mongo::OperationContext* opCtx,
        const mongo::NamespaceString& ns,
        const boost::optional<mongo::UUID>& collUUID) const override {
        MONGO_UNREACHABLE;
    }

    void appendLatencyStats(mongo::OperationContext* opCtx,
                            const mongo::NamespaceString& nss,
                            bool includeHistograms,
                            mongo::BSONObjBuilder* builder) const override {
        MONGO_UNREACHABLE;
    }

    mongo::Status appendStorageStats(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& nss,
        const mongo::StorageStatsSpec& spec,
        mongo::BSONObjBuilder* builder,
        const boost::optional<mongo::BSONObj>& filterObj) const override {
        MONGO_UNREACHABLE;
    }

    mongo::Status appendRecordCount(mongo::OperationContext* opCtx,
                                    const mongo::NamespaceString& nss,
                                    mongo::BSONObjBuilder* builder) const override {
        MONGO_UNREACHABLE;
    }

    mongo::Status appendQueryExecStats(mongo::OperationContext* opCtx,
                                       const mongo::NamespaceString& nss,
                                       mongo::BSONObjBuilder* builder) const override {
        MONGO_UNREACHABLE;
    }

    void appendOperationStats(mongo::OperationContext* opCtx,
                              const mongo::NamespaceString& nss,
                              mongo::BSONObjBuilder* builder) const override {
        MONGO_UNREACHABLE;
    }

    mongo::BSONObj getCollectionOptions(mongo::OperationContext* opCtx,
                                        const mongo::NamespaceString& nss) override {
        MONGO_UNREACHABLE;
    }

    mongo::query_shape::CollectionType getCollectionType(mongo::OperationContext* opCtx,
                                                         const mongo::NamespaceString& nss) final {
        MONGO_UNREACHABLE;
    }

    void renameIfOptionsAndIndexesHaveNotChanged(
        mongo::OperationContext* opCtx,
        const mongo::NamespaceString& sourceNs,
        const mongo::NamespaceString& targetNs,
        bool dropTarget,
        bool stayTemp,
        const mongo::BSONObj& originalCollectionOptions,
        const std::list<mongo::BSONObj>& originalIndexes) override {
        MONGO_UNREACHABLE;
    }

    // Initializes _configDatabase and _configCollection.
    virtual void initConfigCollection();

    virtual void ensureCollectionExists(const mongo::NamespaceString& ns) {
        getCollection(getDb(ns.dbName()), ns.coll().toString());
    }

    void createCollection(mongo::OperationContext* opCtx,
                          const mongo::DatabaseName& dbName,
                          const mongo::BSONObj& cmdObj) override {
        MONGO_UNREACHABLE;
    }

    void createTempCollection(mongo::OperationContext* opCtx,
                              const mongo::NamespaceString& nss,
                              const mongo::BSONObj& collectionOptions,
                              boost::optional<mongo::ShardId> dataShard) override {
        MONGO_UNREACHABLE;
    }

    void createIndexesOnEmptyCollection(mongo::OperationContext* opCtx,
                                        const mongo::NamespaceString& ns,
                                        const std::vector<mongo::BSONObj>& indexSpecs) override {
        MONGO_UNREACHABLE;
    }

    void dropCollection(mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) override {
        MONGO_UNREACHABLE;
    }

    void dropTempCollection(mongo::OperationContext* opCtx,
                            const mongo::NamespaceString& nss) override {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> preparePipelineForExecution(
        mongo::Pipeline* pipeline,
        mongo::ShardTargetingPolicy shardTargetingPolicy = mongo::ShardTargetingPolicy::kAllowed,
        boost::optional<mongo::BSONObj> readConcern = boost::none) override;

    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter> preparePipelineForExecution(
        const mongo::AggregateCommandRequest& aggRequest,
        mongo::Pipeline* pipeline,
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        boost::optional<mongo::BSONObj> shardCursorsSortSpec = boost::none,
        mongo::ShardTargetingPolicy shardTargetingPolicy = mongo::ShardTargetingPolicy::kAllowed,
        boost::optional<mongo::BSONObj> readConcern = boost::none) override {
        MONGO_UNREACHABLE;
    }

    mongo::BSONObj preparePipelineAndExplain(mongo::Pipeline* ownedPipeline,
                                             mongo::ExplainOptions::Verbosity verbosity) override {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter>
    attachCursorSourceToPipelineForLocalRead(
        mongo::Pipeline* pipeline,
        boost::optional<const mongo::AggregateCommandRequest&> aggRequest = boost::none) override {
        MONGO_UNREACHABLE;
    }

    std::vector<mongo::BSONObj> getCurrentOps(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        CurrentOpConnectionsMode connMode,
        CurrentOpSessionsMode sessionMode,
        CurrentOpUserMode userMode,
        CurrentOpTruncateMode truncateMode,
        CurrentOpCursorMode cursorMode) const override {
        MONGO_UNREACHABLE;
    }

    std::string getShardName(mongo::OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    boost::optional<mongo::ShardId> getShardId(mongo::OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    bool inShardedEnvironment(mongo::OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    std::string getHostAndPort(mongo::OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    std::vector<mongo::FieldPath> collectDocumentKeyFieldsActingAsRouter(
        mongo::OperationContext*, const mongo::NamespaceString& nss) const override;

    boost::optional<mongo::Document> lookupSingleDocument(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& nss,
        mongo::UUID collectionUUID,
        const mongo::Document& documentKey,
        boost::optional<mongo::BSONObj> readConcern) override {
        MONGO_UNREACHABLE;
    }

    boost::optional<mongo::Document> lookupSingleDocumentLocally(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& nss,
        const mongo::Document& documentKey) override {
        MONGO_UNREACHABLE;
    }

    std::vector<mongo::GenericCursor> getIdleCursors(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        CurrentOpUserMode userMode) const override {
        MONGO_UNREACHABLE;
    }

    mongo::BackupCursorState openBackupCursor(
        mongo::OperationContext* opCtx,
        const mongo::StorageEngine::BackupOptions& options) override {
        return mongo::BackupCursorState{mongo::UUID::gen(), boost::none, nullptr, {}};
    }

    void closeBackupCursor(mongo::OperationContext* opCtx, const mongo::UUID& backupId) override {}

    mongo::BackupCursorExtendState extendBackupCursor(mongo::OperationContext* opCtx,
                                                      const mongo::UUID& backupId,
                                                      const mongo::Timestamp& extendTo) override {
        return {{}};
    }

    std::vector<mongo::BSONObj> getMatchingPlanCacheEntryStats(
        mongo::OperationContext*,
        const mongo::NamespaceString&,
        const mongo::MatchExpression*) const override {
        MONGO_UNREACHABLE;
    }

    SupportingUniqueIndex fieldsHaveSupportingUniqueIndex(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& nss,
        const std::set<mongo::FieldPath>& fieldPaths) const override;

    boost::optional<mongo::DatabaseVersion> refreshAndGetDatabaseVersion(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::DatabaseName& dbName) const override {
        return boost::none;
    }

    void checkRoutingInfoEpochOrThrow(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                      const mongo::NamespaceString&,
                                      mongo::ChunkVersion) const override {
        uasserted(76971, "Unexpected check of routing table");
    }

    DocumentKeyResolutionMetadata ensureFieldsUniqueOrResolveDocumentKey(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        boost::optional<std::set<mongo::FieldPath>> fieldPaths,
        boost::optional<mongo::ChunkVersion> targetCollectionPlacementVersion,
        const mongo::NamespaceString& outputNs) const override;

    std::unique_ptr<ScopedExpectUnshardedCollection> expectUnshardedCollectionInScope(
        mongo::OperationContext* opCtx,
        const mongo::NamespaceString& nss,
        const boost::optional<mongo::DatabaseVersion>& dbVersion) override {
        class ScopedExpectUnshardedCollectionNoop : public ScopedExpectUnshardedCollection {
        public:
            ScopedExpectUnshardedCollectionNoop() = default;
        };

        return std::make_unique<ScopedExpectUnshardedCollectionNoop>();
    }

    std::unique_ptr<mongo::TemporaryRecordStore> createTemporaryRecordStore(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        mongo::KeyFormat keyFormat) const override {
        MONGO_UNREACHABLE;
    }

    void writeRecordsToRecordStore(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                   mongo::RecordStore* rs,
                                   std::vector<mongo::Record>* records,
                                   const std::vector<mongo::Timestamp>& ts) const override {
        MONGO_UNREACHABLE;
    }

    mongo::Document readRecordFromRecordStore(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        mongo::RecordStore* rs,
        mongo::RecordId rID) const override {
        MONGO_UNREACHABLE;
    }

    void deleteRecordFromRecordStore(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                                     mongo::RecordStore* rs,
                                     mongo::RecordId rID) const override {
        MONGO_UNREACHABLE;
    }

    void truncateRecordStore(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                             mongo::RecordStore* rs) const override {
        MONGO_UNREACHABLE;
    }

    // Fetches collection information into the cache (if needed).
    void fetchCollection(mongo::NamespaceString nss);

    // Tests the connection to the target. Might throw exceptions.
    virtual void testConnection(const mongo::NamespaceString& nss);

    // Returns the uri used to connect.
    virtual const mongocxx::uri& uri() {
        return *_uri;
    }

private:
    // Encapsulates metadata for a collection.
    struct CollectionInfo {
        std::unique_ptr<mongocxx::collection> collection;
        std::vector<mongo::BSONObj> indexes;
        boost::optional<mongo::ShardKeyPattern> shardKeyPattern;
    };

    std::unique_ptr<mongocxx::database> createDb(const std::string& dbName);
    std::unique_ptr<mongocxx::collection> createCollection(const mongocxx::database& db,
                                                           const std::string& collName);

    mongocxx::database* getExistingDb(const mongo::DatabaseName& dbName) const;
    CollectionInfo* getExistingCollection(const mongocxx::database& db,
                                          const std::string& collName) const;
    CollectionInfo* getExistingCollection(const mongo::NamespaceString& ns) const {
        return getExistingCollection(*getExistingDb(ns.dbName()), ns.coll().toString());
    }

    mongocxx::database* getDb(const mongo::DatabaseName& dbName);
    CollectionInfo* getCollection(mongocxx::database* db, const std::string& collName);
    CollectionInfo* getCollection(const mongo::NamespaceString& ns) {
        return getCollection(getDb(ns.dbName()), ns.coll().toString());
    }

    // Processes the `run_command` reply from the insert or update operation and throws
    // a `mongocxx::bulk_write_exception` if the reply includes an error.
    void processUpsertReply(const bsoncxx::document::value& reply) const;

    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri;
    std::unique_ptr<mongocxx::client> _client;

    boost::optional<bool> _isInstanceSharded;
    std::unique_ptr<mongocxx::database> _configDatabase;
    std::unique_ptr<mongocxx::collection> _configCollection;
    // The database cache as a map from database name to 'database' object.
    mongo::stdx::unordered_map<std::string, std::unique_ptr<mongocxx::database>> _databaseCache;
    // The collecion cache as a map from db name & collName pair to a CollectionInfo object.
    mongo::stdx::unordered_map<std::pair<std::string, std::string>, std::unique_ptr<CollectionInfo>>
        _collectionCache;
};

}  // namespace streams
