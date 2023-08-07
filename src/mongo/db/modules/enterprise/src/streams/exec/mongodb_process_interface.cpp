/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/mongodb_process_interface.h"

#include <stdexcept>

#include "mongo/db/pipeline/process_interface/common_process_interface.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

namespace {

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
    _database = std::make_unique<mongocxx::database>(_client->database(options.database));
    _collection = std::make_unique<mongocxx::collection>(_database->collection(options.collection));
}

std::unique_ptr<MongoProcessInterface::WriteSizeEstimator>
MongoDBProcessInterface::getWriteSizeEstimator(OperationContext* opCtx,
                                               const NamespaceString& ns) const {
    return std::make_unique<CommonProcessInterface::TargetPrimaryWriteSizeEstimator>();
}

Status MongoDBProcessInterface::insert(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const NamespaceString& ns,
    std::unique_ptr<write_ops::InsertCommandRequest> insertCommand,
    const WriteConcernOptions& wc,
    boost::optional<OID> oid) {
    dassert(!oid);

    // TODO: Catch exceptions in MergeOperator.
    mongocxx::options::bulk_write writeOptions;
    writeOptions.ordered(true);
    // We ignore wc and use specific write concern for all write operations.
    writeOptions.write_concern(getWriteConcern());
    auto bulkWriteRequest = _collection->create_bulk_write(writeOptions);
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

    // TODO: Catch exceptions in MergeOperator.
    mongocxx::options::bulk_write writeOptions;
    writeOptions.ordered(true);
    // We ignore wc and use specific write concern for all write operations.
    writeOptions.write_concern(getWriteConcern());
    auto bulkWriteRequest = _collection->create_bulk_write(writeOptions);

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

}  // namespace streams
