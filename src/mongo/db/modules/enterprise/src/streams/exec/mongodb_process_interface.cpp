/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/mongodb_process_interface.h"

#include <stdexcept>

#include "mongo/db/pipeline/process_interface/common_process_interface.h"
#include "mongo/logv2/log.h"
#include "mongo/util/database_name_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

static constexpr size_t kMaxDatabaseCacheSize = 100;
static constexpr size_t kMaxCollectionCacheSize = 100;

// This failpoint throws an exception after a successful call to
// failAfterRemoteInsertSucceeds.
MONGO_FAIL_POINT_DEFINE(failAfterRemoteInsertSucceeds);

// Returns the default mongocxx::write_concern we use for all write operations.
mongocxx::write_concern getWriteConcern() {
    mongocxx::write_concern writeConcern;
    writeConcern.journal(true);
    writeConcern.acknowledge_level(mongocxx::write_concern::level::k_majority);
    // TODO: Make the timeout configurable.
    writeConcern.majority(/*timeout*/ stdx::chrono::milliseconds(60 * 1000));
    return writeConcern;
}

}  // namespace

MongoDBProcessInterface::MongoDBProcessInterface(const MongoCxxClientOptions& options)
    : MongoProcessInterface(nullptr) {
    _instance = getMongocxxInstance(options.svcCtx);
    _uri = std::make_unique<mongocxx::uri>(options.uri);
    _client = std::make_unique<mongocxx::client>(*_uri, options.toMongoCxxClientOptions());
}

MongoDBProcessInterface::MongoDBProcessInterface() : MongoProcessInterface(nullptr) {}

std::unique_ptr<MongoProcessInterface::WriteSizeEstimator>
MongoDBProcessInterface::getWriteSizeEstimator(OperationContext* opCtx,
                                               const NamespaceString& ns) const {
    return std::make_unique<CommonProcessInterface::TargetPrimaryWriteSizeEstimator>();
}

mongocxx::database& MongoDBProcessInterface::getDb(const mongo::DatabaseName& dbName) {
    auto dbNameStr = DatabaseNameUtil::serialize(dbName, SerializationContext());
    tassert(8143709, "The database name must not be empty", !dbNameStr.empty());

    auto dbIt = _databaseCache.find(dbNameStr);
    if (dbIt == _databaseCache.end()) {
        uassert(8143705,
                "Too many unique databases: {}"_format(_databaseCache.size()),
                _databaseCache.size() < kMaxDatabaseCacheSize);
        bool inserted = false;
        std::tie(dbIt, inserted) = _databaseCache.emplace(dbNameStr, _client->database(dbNameStr));
        tassert(8143704, "Failed to insert a database into cache: {}"_format(dbNameStr), inserted);
    }

    return dbIt->second;
}

mongocxx::collection& MongoDBProcessInterface::getCollection(const mongocxx::database& db,
                                                             const std::string& collName) {
    tassert(8143706, "The collection name must not be empty", !collName.empty());

    // We maintain the collecion cache as a map from db name & collName pair to 'collection',
    auto nsKey = std::make_pair(std::string(db.name()), collName);
    auto collIt = _collectionCache.find(nsKey);
    if (collIt == _collectionCache.end()) {
        uassert(8143707,
                "Too many unique collections: {}"_format(_collectionCache.size()),
                _collectionCache.size() < kMaxCollectionCacheSize);
        bool inserted = false;
        std::tie(collIt, inserted) = _collectionCache.emplace(nsKey, db.collection(collName));
        tassert(8143708,
                "Failed to insert a collection into cache: {}.{}"_format(std::string(db.name()),
                                                                         collName),
                inserted);
    }

    return collIt->second;
}

Status MongoDBProcessInterface::insert(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const NamespaceString& ns,
    std::unique_ptr<write_ops::InsertCommandRequest> insertCommand,
    const WriteConcernOptions& wc,
    boost::optional<OID> oid) {
    dassert(!oid);

    mongocxx::options::bulk_write writeOptions;
    writeOptions.ordered(true);
    // We ignore wc and use specific write concern for all write operations.
    writeOptions.write_concern(getWriteConcern());
    // We capture the return value by & to avoid deep copying.
    auto& collection = getCollection(ns);
    auto bulkWriteRequest = collection.create_bulk_write(writeOptions);
    for (auto& obj : insertCommand->getDocuments()) {
        mongocxx::model::insert_one insertRequest(toBsoncxxDocument(obj));
        bulkWriteRequest.append(std::move(insertRequest));
    }

    auto bulkWriteResponse = bulkWriteRequest.execute();

    // If no exceptions were thrown, it means that the operation succeeded.
    dassert(bulkWriteResponse);
    return Status::OK();
}

StatusWith<MongoProcessInterface::UpdateResult> MongoDBProcessInterface::update(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const NamespaceString& ns,
    std::unique_ptr<write_ops::UpdateCommandRequest> updateCommand,
    const WriteConcernOptions& wc,
    UpsertType upsert,
    bool multi,
    boost::optional<OID> oid) {
    dassert(!oid);
    dassert(!multi);

    mongocxx::options::bulk_write writeOptions;
    writeOptions.ordered(true);
    // We ignore wc and use specific write concern for all write operations.
    writeOptions.write_concern(getWriteConcern());
    // We capture the return value by & to avoid deep copying.
    auto& collection = getCollection(ns);
    auto bulkWriteRequest = collection.create_bulk_write(writeOptions);

    const auto& updates = updateCommand->getUpdates();
    for (auto& updateOp : updates) {
        dassert(!updateOp.getC().has_value());

        auto& updateModification = updateOp.getU();
        if (updateModification.type() == write_ops::UpdateModification::Type::kReplacement) {
            BSONObj updateObj = updateModification.getUpdateReplacement();
            mongocxx::model::replace_one replaceRequest(toBsoncxxDocument(updateOp.getQ()),
                                                        toBsoncxxDocument(updateObj));
            if (upsert == UpsertType::kNone) {
                replaceRequest.upsert(false);
            } else {
                replaceRequest.upsert(true);
            }
            bulkWriteRequest.append(std::move(replaceRequest));
        } else {
            dassert(updateModification.type() == write_ops::UpdateModification::Type::kModifier);
            BSONObj updateObj = updateModification.getUpdateModifier();
            mongocxx::model::update_one updateRequest(toBsoncxxDocument(updateOp.getQ()),
                                                      toBsoncxxDocument(updateObj));
            if (upsert == UpsertType::kNone) {
                updateRequest.upsert(false);
            } else {
                updateRequest.upsert(true);
            }
            bulkWriteRequest.append(std::move(updateRequest));
        }
    }

    auto bulkWriteResponse = bulkWriteRequest.execute();

    // If no exceptions were thrown, it means that the operation succeeded.
    dassert(bulkWriteResponse);
    UpdateResult result;
    result.nMatched = bulkWriteResponse->matched_count();
    result.nModified = bulkWriteResponse->modified_count();

    if (MONGO_unlikely(failAfterRemoteInsertSucceeds.shouldFail())) {
        // We use runtime_error because invariant() will crash the process, and we just
        // want this streamProcessor to be killed. tassert and uassert throw DBExceptions
        // which are handled in the MergeOperator further up the callstack.
        throw std::runtime_error("failAfterRemoteInsertSucceeds failpoint");
    }

    return StatusWith(std::move(result));
}

mongocxx::cursor MongoDBProcessInterface::query(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const mongo::NamespaceString& ns,
    const BSONObj& filter) {
    mongocxx::options::find findOptions;
    findOptions.batch_size(100);
    findOptions.cursor_type(mongocxx::cursor::type::k_non_tailable);
    // We capture the return value by & to avoid deep copying.
    auto& collection = getCollection(ns);
    auto cursor = collection.find(toBsoncxxDocument(filter), findOptions);
    return cursor;
}

}  // namespace streams
