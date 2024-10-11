/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/mongodb_process_interface.h"

#include <bsoncxx/types.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/server_error_code.hpp>
#include <stdexcept>

#include "mongo/bson/bsontypes.h"
#include "mongo/db/database_name.h"
#include "mongo/db/index/index_constants.h"
#include "mongo/db/pipeline/process_interface/common_process_interface.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/util/database_name_util.h"
#include "streams/exec/document_source_remote_db_cursor.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using namespace mongo;
using namespace fmt::literals;

namespace {

static constexpr size_t kMaxDatabaseCacheSize = 1000;
static constexpr size_t kMaxCollectionCacheSize = 1000;

// This failpoint throws an exception after a successful call to
// failAfterRemoteInsertSucceeds.
MONGO_FAIL_POINT_DEFINE(failAfterRemoteInsertSucceeds);


// Returns the default mongocxx::write_concern we use for all write operations.
WriteConcernOptions getWriteConcern() {
    // TODO: Make the timeout configurable.
    return WriteConcernOptions(WriteConcernOptions::kMajority,
                               WriteConcernOptions::SyncMode::JOURNAL,
                               Milliseconds(60 * 1000));
}

// The implementation of this function largely matches the implementation of the same function in
// mongos_process_interface.cpp.
MongoProcessInterface::SupportingUniqueIndex supportsUniqueKey(
    const BSONObj& index, const std::set<FieldPath>& uniqueKeyPaths) {
    // SERVER-5335: The _id index does not report to be unique, but in fact is unique.
    auto isIdIndex =
        index[IndexDescriptor::kIndexNameFieldName].String() == IndexConstants::kIdIndexName;
    bool supports =
        (isIdIndex || index.getBoolField(IndexDescriptor::kUniqueFieldName)) &&
        !index.hasField(IndexDescriptor::kPartialFilterExprFieldName) &&
        CommonProcessInterface::keyPatternNamesExactPaths(
            index.getObjectField(IndexDescriptor::kKeyPatternFieldName), uniqueKeyPaths);
    if (!supports) {
        return MongoProcessInterface::SupportingUniqueIndex::None;
    }
    return index.getBoolField(IndexDescriptor::kSparseFieldName)
        ? MongoProcessInterface::SupportingUniqueIndex::NotNullish
        : MongoProcessInterface::SupportingUniqueIndex::Full;
}

}  // namespace

MongoDBProcessInterface::MongoDBProcessInterface(const MongoCxxClientOptions& options)
    : MongoProcessInterface(nullptr) {
    _instance = getMongocxxInstance(options.svcCtx);
    _uri = makeMongocxxUri(options.uri);
    _client = std::make_unique<mongocxx::client>(*_uri, options.toMongoCxxClientOptions());
}

MongoDBProcessInterface::MongoDBProcessInterface() : MongoProcessInterface(nullptr) {}

std::unique_ptr<MongoProcessInterface::WriteSizeEstimator>
MongoDBProcessInterface::getWriteSizeEstimator(OperationContext* opCtx,
                                               const NamespaceString& ns) const {
    return std::make_unique<CommonProcessInterface::TargetPrimaryWriteSizeEstimator>();
}

std::unique_ptr<mongocxx::database> MongoDBProcessInterface::createDb(const std::string& dbName) {
    return std::make_unique<mongocxx::database>(_client->database(dbName));
}

std::unique_ptr<mongocxx::collection> MongoDBProcessInterface::createCollection(
    const mongocxx::database& db, const std::string& collName) {
    return std::make_unique<mongocxx::collection>(db.collection(collName));
}

void MongoDBProcessInterface::initConfigCollection() {
    invariant(!_configCollection);

    auto ns = CollectionType::ConfigNS;
    auto dbNameStr = DatabaseNameUtil::serialize(ns.dbName(), SerializationContext());
    auto collName = ns.coll().toString();
    _configDatabase = createDb(dbNameStr);
    _configCollection = createCollection(*_configDatabase, collName);
}

mongocxx::database* MongoDBProcessInterface::getExistingDb(
    const mongo::DatabaseName& dbName) const {
    auto dbNameStr = DatabaseNameUtil::serialize(dbName, SerializationContext());
    tassert(8143709, "The database name must not be empty", !dbNameStr.empty());

    auto dbIt = _databaseCache.find(dbNameStr);
    if (dbIt != _databaseCache.end()) {
        return dbIt->second.get();
    }
    return nullptr;
}

mongocxx::database* MongoDBProcessInterface::getDb(const mongo::DatabaseName& dbName) {
    auto existingDb = getExistingDb(dbName);
    if (existingDb) {
        return existingDb;
    }

    auto dbNameStr = DatabaseNameUtil::serialize(dbName, SerializationContext());
    tassert(8186201, "The database name must not be empty", !dbNameStr.empty());
    uassert(ErrorCodes::StreamProcessorTooManyOutputTargets,
            "Too many unique databases: {}"_format(_databaseCache.size()),
            _databaseCache.size() < kMaxDatabaseCacheSize);

    auto [dbIt, inserted] = _databaseCache.emplace(dbNameStr, createDb(dbNameStr));
    tassert(8143704, "Failed to insert a database into cache: {}"_format(dbNameStr), inserted);
    return dbIt->second.get();
}

MongoDBProcessInterface::CollectionInfo* MongoDBProcessInterface::getExistingCollection(
    const mongocxx::database& db, const std::string& collName) const {
    tassert(8143706, "The collection name must not be empty", !collName.empty());

    // We maintain the collecion cache as a map from db name & collName pair to 'collection',
    auto nsKey = std::make_pair(db.name().to_string(), collName);
    auto collIt = _collectionCache.find(nsKey);
    if (collIt != _collectionCache.end()) {
        return collIt->second.get();
    }
    return nullptr;
}

MongoDBProcessInterface::CollectionInfo* MongoDBProcessInterface::getCollection(
    mongocxx::database* db, const std::string& collName) {
    auto existingCollInfo = getExistingCollection(*db, collName);
    if (existingCollInfo) {
        return existingCollInfo;
    }

    tassert(8186207, "The collection name must not be empty", !collName.empty());
    uassert(8143707,
            "Too many unique collections: {}"_format(_collectionCache.size()),
            _collectionCache.size() < kMaxCollectionCacheSize);

    auto collInfo = std::make_unique<CollectionInfo>();
    collInfo->collection = createCollection(*db, collName);
    for (auto index : collInfo->collection->list_indexes()) {
        collInfo->indexes.push_back(fromBsoncxxDocument(index));
    }

    if (!_isInstanceSharded) {
        auto helloResponse = fromBsoncxxDocument(db->run_command(make_document(kvp("hello", "1"))));
        auto msgElement = helloResponse["msg"];
        uassert(8429101,
                "Unexpected hello response: {}"_format(helloResponse.toString()),
                !msgElement || msgElement.type() == BSONType::String);
        if (msgElement && msgElement.String() == "isdbgrid") {
            _isInstanceSharded = true;
        } else {
            _isInstanceSharded = false;
        }
    }
    invariant(_isInstanceSharded);

    if (*_isInstanceSharded && !_configCollection) {
        initConfigCollection();
        invariant(_configCollection);
    }

    if (_configCollection) {
        // Read shard key, if any, for the collection from config.collections collection.
        auto nss = NamespaceStringUtil::serialize(
            getNamespaceString(db->name().to_string(), collName), SerializationContext());
        auto cursor = _configCollection->find(make_document(kvp(kIdFieldName, nss)));
        std::vector<BSONObj> configDocs;
        for (const auto& doc : cursor) {
            configDocs.emplace_back(fromBsoncxxDocument(doc));
        }
        uassert(8186210,
                "Found more than 1 entries in config.collections for {}"_format(nss),
                configDocs.size() <= 1);
        if (!configDocs.empty()) {
            const auto& collectionConfig = configDocs[0];
            auto shardKey = collectionConfig[CollectionType::kKeyPatternFieldName];
            if (!shardKey.eoo()) {
                collInfo->shardKeyPattern.emplace(shardKey.Obj());
            }
        }
    }

    auto collInfoPtr = collInfo.get();
    auto nsKey = std::make_pair(db->name().to_string(), collName);
    auto [collIt, inserted] = _collectionCache.emplace(nsKey, std::move(collInfo));
    tassert(
        8186204,
        "Failed to insert a collection into cache: {}.{}"_format(db->name().to_string(), collName),
        inserted);
    return collInfoPtr;
}

Status MongoDBProcessInterface::insert(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const NamespaceString& ns,
    std::unique_ptr<write_ops::InsertCommandRequest> insertCommand,
    const WriteConcernOptions& wc,
    boost::optional<OID> oid) {
    dassert(!oid);

    // We ignore the 'wc' argument and use specific write concern for all write operations.
    insertCommand->setWriteConcern(getWriteConcern());
    insertCommand->getWriteCommandRequestBase().setOrdered(true);
    auto cmdObj = insertCommand->toBSON();
    auto* db = getDb(ns.dbName());

    // Lets the operation_exception be thrown if the operation fails.
    auto reply = db->run_command({toBsoncxxValue(std::move(cmdObj))});
    processUpsertReply(reply);
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

    //
    // Constructs a command object from the update command and the write concern.
    //
    // We ignore legacy runtime constants.
    updateCommand->setLegacyRuntimeConstants(boost::none);
    updateCommand->getWriteCommandRequestBase().setOrdered(true);
    // We ignore the 'wc' argument and use specific write concern for all write operations.
    updateCommand->setWriteConcern(getWriteConcern());
    auto cmdObj = updateCommand->toBSON();

    //
    // Executes the command.
    //
    auto db = getDb(ns.dbName());
    // Lets the operation_exception be thrown if the operation fails.
    auto reply = db->run_command({toBsoncxxValue(std::move(cmdObj))});
    processUpsertReply(reply);

    return StatusWith(UpdateResult{
        .nMatched = reply[write_ops::WriteCommandReplyBase::kNFieldName.toString()].get_int32(),
        .nModified =
            reply[write_ops::UpdateCommandReply::kNModifiedFieldName.toString()].get_int32()});
}

void MongoDBProcessInterface::processUpsertReply(const bsoncxx::document::value& reply) const {
    if (reply.find(kWriteErrorsFieldName) != reply.end()) {
        // We use 'ordered' write and so the first error should be the only error.
        auto writeError = reply[kWriteErrorsFieldName][0];
        auto replyClone = reply;
        throw mongocxx::bulk_write_exception{
            std::error_code(
                writeError[write_ops::WriteError::kCodeFieldName.toString()].get_int32(),
                mongocxx::server_error_category()),
            std::move(replyClone),
            writeError[write_ops::WriteError::kErrmsgFieldName.toString()]
                .get_utf8()
                .value.to_string()};
    }

    if (MONGO_unlikely(failAfterRemoteInsertSucceeds.shouldFail())) {
        // We use runtime_error because invariant() will crash the process, and we just
        // want this streamProcessor to be killed. tassert and uassert throw DBExceptions
        // which are handled in the MergeOperator further up the callstack.
        throw std::runtime_error("failAfterRemoteInsertSucceeds failpoint");
    }
}

std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter>
MongoDBProcessInterface::preparePipelineForExecution(mongo::Pipeline* pipeline,
                                                     mongo::ShardTargetingPolicy,
                                                     boost::optional<mongo::BSONObj>) {
    // Creates an empty pipeline with the same context as the given pipeline.
    MakePipelineOptions opts;
    opts.optimize = false;
    opts.attachCursorSource = false;
    opts.shardTargetingPolicy = ShardTargetingPolicy::kNotAllowed;
    auto newPipeline = Pipeline::makePipeline(std::vector<BSONObj>{}, pipeline->getContext(), opts);

    // The sole document source for the new pipeline is a remote db cursor. The
    // DocumentSourceRemoteDbCursor runs the 'pipeline' on the remote db server.
    newPipeline->getSources().push_back(DocumentSourceRemoteDbCursor::create(this, pipeline));
    return newPipeline;
}

// The implementation of this function largely matches the implementation of the same function in
// mongos_process_interface.cpp.
MongoProcessInterface::SupportingUniqueIndex
MongoDBProcessInterface::fieldsHaveSupportingUniqueIndex(
    const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
    const mongo::NamespaceString& nss,
    const std::set<FieldPath>& fieldPaths) const {
    auto collInfo = getExistingCollection(nss);
    tassert(8186206,
            "Could not find the collection instance for {}"_format(nss.toStringForErrorMsg()),
            collInfo);
    if (collInfo->indexes.empty()) {
        // The collection does not exist.
        return fieldPaths == std::set<FieldPath>{kIdFieldName} ? SupportingUniqueIndex::Full
                                                               : SupportingUniqueIndex::None;
    }
    return std::accumulate(collInfo->indexes.begin(),
                           collInfo->indexes.end(),
                           SupportingUniqueIndex::None,
                           [&fieldPaths](auto result, const auto& index) {
                               return std::max(result, supportsUniqueKey(index, fieldPaths));
                           });
}

// The implementation of this function largely matches the implementation of the same function in
// mongos_process_interface.cpp.
MongoDBProcessInterface::DocumentKeyResolutionMetadata
MongoDBProcessInterface::ensureFieldsUniqueOrResolveDocumentKey(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    boost::optional<std::set<FieldPath>> fieldPaths,
    boost::optional<ChunkVersion> targetCollectionPlacementVersion,
    const NamespaceString& outputNs) const {
    static const auto kNoDb = DatabaseNameUtil::deserialize(
        /*tenantId=*/boost::none, kNoDbDbName, SerializationContext());
    uassert(8186208,
            "ensureFieldsUniqueOrResolveDocumentKey() called for the dummy DbName",
            outputNs.dbName() != kNoDb);

    if (fieldPaths) {
        auto supportingUniqueIndex = fieldsHaveSupportingUniqueIndex(expCtx, outputNs, *fieldPaths);
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "Cannot find index to verify that join fields will be unique",
                supportingUniqueIndex != SupportingUniqueIndex::None);
        return {*fieldPaths, boost::none, supportingUniqueIndex};
    }

    auto docKeyPaths = collectDocumentKeyFieldsActingAsRouter(expCtx->opCtx, outputNs);
    return {std::set<FieldPath>(std::make_move_iterator(docKeyPaths.begin()),
                                std::make_move_iterator(docKeyPaths.end())),
            boost::none,
            SupportingUniqueIndex::Full};
}

void MongoDBProcessInterface::fetchCollection(mongo::NamespaceString nss) {
    getCollection(nss);
}

void MongoDBProcessInterface::testConnection(const mongo::NamespaceString& nss) {
    auto db = getDb(nss.dbName());
    callHello(*db);
    fetchCollection(nss);
}

// The implementation of this function largely matches the implementation of the same function in
// common_process_interface.cpp.
std::vector<mongo::FieldPath> MongoDBProcessInterface::collectDocumentKeyFieldsActingAsRouter(
    mongo::OperationContext*, const mongo::NamespaceString& nss) const {
    auto collInfo = getExistingCollection(nss);
    tassert(8186205,
            "Could not find the collection instance for {}"_format(nss.toStringForErrorMsg()),
            collInfo);
    if (*_isInstanceSharded && collInfo->shardKeyPattern) {
        return CommonProcessInterface::shardKeyToDocumentKeyFields(
            collInfo->shardKeyPattern->getKeyPatternFields());
    }

    // We have no evidence this collection is sharded, so the document key is just _id.
    return {kIdFieldName};
}

}  // namespace streams
