/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "magic_restore.h"

#include <algorithm>
#include <boost/filesystem/operations.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "magic_restore/magic_restore_structs_gen.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/collection_crud/collection_write_path.h"
#include "mongo/db/commands/user_management_commands_gen.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index/index_constants.h"
#include "mongo/db/mongod_options.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/op_observer/op_observer.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/oplog_entry_gen.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/logv2/log.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/grid.h"
#include "mongo/util/exit.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/string_map.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kMagicRestore

namespace mongo {
namespace magic_restore {


BSONStreamReader::BSONStreamReader(std::istream& stream) : _stream(stream) {
    _buffer = std::make_unique<char[]>(BSONObjMaxUserSize);
}

bool BSONStreamReader::hasNext() {
    return _stream.peek() != std::char_traits<char>::eof();
}

BSONObj BSONStreamReader::getNext() {
    // Read the length of the BSON object.
    _stream.read(_buffer.get(), _bsonLengthHeaderSizeBytes);
    auto gcount = _stream.gcount();
    if (!_stream || gcount < _bsonLengthHeaderSizeBytes) {
        LOGV2_FATAL(8290500,
                    "Failed to read BSON length from stream",
                    "bytesRead"_attr = gcount,
                    "totalBytesRead"_attr = _totalBytesRead,
                    "totalObjectsRead"_attr = _totalObjectsRead);
    }
    _totalBytesRead += gcount;

    // The BSON length is always little endian.
    const std::int32_t bsonLength = ConstDataView(_buffer.get()).read<LittleEndian<std::int32_t>>();
    if (bsonLength < BSONObj::kMinBSONLength || bsonLength > BSONObjMaxUserSize) {
        // Error out on invalid length values. Otherwise let invalid BSON data fail in future steps.
        LOGV2_FATAL(
            8290501, "Parsed invalid BSON length in stream", "BSONLength"_attr = bsonLength);
    }
    const auto bytesToRead = bsonLength - _bsonLengthHeaderSizeBytes;
    _stream.read(_buffer.get() + _bsonLengthHeaderSizeBytes, bytesToRead);
    gcount = _stream.gcount();
    _totalBytesRead += gcount;
    if (!_stream || gcount < bytesToRead) {
        // We read a valid BSON object length, but the stream failed or we failed to read the
        // remainder of the object.
        LOGV2_FATAL(8290502,
                    "Failed to read entire BSON object",
                    "expectedLength"_attr = bsonLength,
                    "bytesRead"_attr = gcount + _bsonLengthHeaderSizeBytes,
                    "totalBytesRead"_attr = _totalBytesRead,
                    "totalObjectsRead"_attr = _totalObjectsRead);
    }
    _totalObjectsRead++;
    return BSONObj(_buffer.get());
}

int64_t BSONStreamReader::getTotalBytesRead() {
    return _totalBytesRead;
}

int64_t BSONStreamReader::getTotalObjectsRead() {
    return _totalObjectsRead;
}

void writeOplogEntriesToOplog(OperationContext* opCtx, BSONStreamReader& reader) {
    LOGV2(8290818, "Writing additional PIT oplog entries into oplog");
    auto restoreConfigBytes = reader.getTotalBytesRead();
    AutoGetOplogFastPath oplog(opCtx, OplogAccessMode::kWrite);
    Timestamp latestOplogTs;
    while (reader.hasNext()) {
        BSONObj obj = reader.getNext();
        latestOplogTs = obj["ts"].timestamp();
        writeConflictRetry(opCtx,
                           "Inserting into oplog for a PIT magic restore",
                           NamespaceString::kRsOplogNamespace,
                           [&]() {
                               WriteUnitOfWork wuow(opCtx);
                               uassertStatusOK(
                                   collection_internal::insertDocument(opCtx,
                                                                       oplog.getCollection(),
                                                                       InsertStatement{obj},
                                                                       /*opDebug=*/nullptr));

                               wuow.commit();
                           });
    }
    // The system is running with the oplog visibility manager. However,
    // magic restore runs before replication starts, writes are not being
    // timestamped. Force visibility forward to the top of the oplog after inserting all
    // additional oplog entries. The visibility needs to be updated for oplog application to handle
    // oplog entries that require traversing a prevOpTime chain, such as in transactions, since
    // these traversals read from the oplog. These reads will respect the visibility timestamp.
    const bool orderedCommit = true;
    auto oplogRs = oplog.getCollection()->getRecordStore();
    auto storageEngine = opCtx->getServiceContext()->getStorageEngine();
    uassertStatusOK(
        storageEngine->oplogDiskLocRegister(opCtx, oplogRs, latestOplogTs, orderedCommit));
    // When logging the BSONStreamReader metrics for oplog entries, account for the restore
    // configuration document.
    LOGV2(8290702,
          "All additional oplog entries have been inserted",
          "total entries inserted"_attr = reader.getTotalObjectsRead() - 1,
          "total bytes inserted"_attr = reader.getTotalBytesRead() - restoreConfigBytes);
}


void executeCredentialsCommand(OperationContext* opCtx,
                               const BSONObj& cmd,
                               repl::StorageInterface* storageInterface) {

    // For commands, the first element must be the command name.
    auto cmdName = cmd.firstElementFieldNameStringData();

    DBDirectClient dbClient(opCtx);
    OpMsgRequest request;
    request.body = cmd;
    auto replyStatus =
        getStatusFromWriteCommandReply(dbClient.runCommand(request)->getCommandReply());

    // If the create operation succeeded, we can return early.
    if (replyStatus.isOK()) {
        return;
    }

    // The user management command layer catches duplicate key errors and returns the 51002/51003
    // error codes instead.
    if (replyStatus.code() != 51002 && replyStatus.code() != 51003) {
        LOGV2_FATAL(8291701,
                    "Failed to create new role or user for magic restore",
                    "replyStatus"_attr = replyStatus);
    }

    // If there was a duplicate key, we must convert the create operation into an update.
    auto roleOrUser = cmd.getField(cmdName);
    auto bob = BSONObjBuilder();
    bob.appendAs(roleOrUser, cmdName.toString().replace(0, 6, "update"));
    bob.appendElements(cmd.removeField(cmdName));

    request.body = bob.obj();
    replyStatus = getStatusFromWriteCommandReply(dbClient.runCommand(request)->getCommandReply());
    if (!replyStatus.isOK()) {
        LOGV2_FATAL(8291702,
                    "Failed to update role or user for magic restore",
                    "replyStatus"_attr = replyStatus);
    }
}

void upsertAutomationCredentials(OperationContext* opCtx,
                                 const AutomationCredentials& autoCreds,
                                 repl::StorageInterface* storageInterface) {
    LOGV2(8291700, "Creating roles and users in magic restore for automation agent");
    // Check the auth collections exist prior to performing the upsert.
    checkInternalCollectionExists(opCtx, NamespaceString::kAdminRolesNamespace);
    checkInternalCollectionExists(opCtx, NamespaceString::kAdminUsersNamespace);
    // Note that for both the createRole and createUser commands below, we need
    // to ensure they can successfully parse into the corresponding IDL commands. In the magic
    // restore IDL specification, we needed to mark the arrays as containing BSONObjs rather than
    // commands, as commands cannot be specified in array types. Therefore, we need to do the
    // error-checking here.
    for (auto& createRole : autoCreds.getCreateRoleCommands()) {
        mongo::CreateRoleCommand::parse(IDLParserContext("CreateRoleCommand"), createRole);
        executeCredentialsCommand(opCtx, createRole, storageInterface);
    }

    for (auto& createUser : autoCreds.getCreateUserCommands()) {
        mongo::CreateUserCommand::parse(IDLParserContext("CreateUserCommand"), createUser);
        executeCredentialsCommand(opCtx, createUser, storageInterface);
    }
}

void validateRestoreConfiguration(const RestoreConfiguration* config) {
    // If the restore is PIT, the PIT timestamp must be strictly greater than the maxCheckpointTs,
    // which is the snapshot timestamp of the restored datafiles.
    if (auto pit = config->getPointInTimeTimestamp(); pit) {
        uassert(8290601,
                "The pointInTimeTimestamp must be greater than the maxCheckpointTs.",
                pit > config->getMaxCheckpointTs());
    }

    auto hasShardingFields = config->getShardIdentityDocument() || config->getShardingRename() ||
        config->getBalancerSettings();
    if (hasShardingFields) {
        uassert(
            8290602,
            "If the 'shardIdentityDocument', 'shardingRename', or 'balancerSettings' fields exist "
            "in the restore configuration, the node type must be either 'shard', 'configServer', "
            "or 'configShard'.",
            config->getNodeType() != NodeTypeEnum::kReplicaSet);
        uassert(8290603,
                "If 'shardingRename' exists in the restore configuration, "
                "'shardIdentityDocument' must also be passed in.",
                !config->getShardingRename() || config->getShardIdentityDocument());
    }
}

void truncateLocalDbCollections(OperationContext* opCtx, repl::StorageInterface* storageInterface) {
    LOGV2(8290801, "Truncating local replication metadata");
    fassert(7197101,
            storageInterface->truncateCollection(opCtx, NamespaceString::kSystemReplSetNamespace));
    fassert(8291101,
            storageInterface->truncateCollection(
                opCtx, NamespaceString::kDefaultOplogTruncateAfterPointNamespace));
    fassert(
        8291102,
        storageInterface->truncateCollection(opCtx, NamespaceString::kDefaultMinValidNamespace));
    fassert(8291103,
            storageInterface->truncateCollection(opCtx, NamespaceString::kLastVoteNamespace));
    fassert(8291104,
            storageInterface->truncateCollection(opCtx,
                                                 NamespaceString::kDefaultInitialSyncIdNamespace));
}

void setInvalidMinValid(OperationContext* opCtx, repl::StorageInterface* storageInterface) {
    LOGV2(8290803, "Inserting invalid minvalid document");
    Timestamp timestamp(0, 1);
    fassert(8291105,
            storageInterface->putSingleton(
                opCtx,
                NamespaceString::kDefaultMinValidNamespace,
                {BSON("_id" << OID() << "t" << -1 << "ts" << timestamp), timestamp}));
}

void checkInternalCollectionExists(OperationContext* opCtx, const NamespaceString& nss) {
    AutoGetCollectionForRead autoColl(opCtx, nss);
    if (!autoColl.getCollection()) {
        LOGV2_FATAL(8816900,
                    "Internal replicated collection does not exist on node. Create "
                    "the internal collection with a specified UUID by using the "
                    "systemUuids field in magic restore configuration.",
                    "nss"_attr = nss);
    }
}

void createInternalCollectionsWithUuid(
    OperationContext* opCtx,
    repl::StorageInterface* storageInterface,
    const std::vector<mongo::magic_restore::NamespaceUUIDPair>& nsAndUuids) {
    for (const auto& nsAndUuid : nsAndUuids) {
        const auto& ns = nsAndUuid.getNs();
        const auto& uuid = nsAndUuid.getUuid();

        LOGV2(8816901,
              "Creating internal collection with specified UUID",
              "ns"_attr = ns,
              "uuid"_attr = uuid);
        CollectionOptions collOptions;
        collOptions.uuid = uuid;
        auto res = storageInterface->createCollection(opCtx, ns, collOptions);

        if (res.code() == ErrorCodes::NamespaceExists) {
            LOGV2(8816902,
                  "Internal collection already exists, skipping collection creation",
                  "ns"_attr = ns);
        } else {
            uassertStatusOK(res);
        }
    }
}

bool isConfig(const RestoreConfiguration& restoreConfig) {
    auto nType = restoreConfig.getNodeType();
    return nType == NodeTypeEnum::kConfigShard || nType == NodeTypeEnum::kDedicatedConfigServer;
}

bool isShard(const RestoreConfiguration& restoreConfig) {
    auto nType = restoreConfig.getNodeType();
    return nType == NodeTypeEnum::kConfigShard || nType == NodeTypeEnum::kShard;
}

void updateShardNameMetadata(OperationContext* opCtx,
                             const RestoreConfiguration& restoreConfig,
                             repl::StorageInterface* storageInterface) {
    const auto& shardingRename = restoreConfig.getShardingRename();

    if (!shardingRename) {
        return;
    }

    for (const auto& shardRenameMapping : shardingRename.get()) {
        const auto& srcShardName = shardRenameMapping.getSourceShardName();
        const auto& dstShardName = shardRenameMapping.getDestinationShardName();
        const auto& dstShardConnStr = shardRenameMapping.getDestinationShardConnectionString();
        if (isConfig(restoreConfig)) {
            // Update "primary" in documents of the config.databases collection.
            fassert(8291301,
                    storageInterface->updateDocuments(
                        opCtx,
                        NamespaceString::kConfigDatabasesNamespace,
                        BSON("primary" << srcShardName),
                        {BSON("$set" << BSON("primary" << dstShardName)), Timestamp(0)}));

            // Update "shard" in documents of the config.chunks collection.
            fassert(8291302,
                    storageInterface->updateDocuments(
                        opCtx,
                        NamespaceString::kConfigsvrChunksNamespace,
                        BSON("shard" << srcShardName),
                        {BSON("$set" << BSON("shard" << dstShardName << "history"
                                                     << BSON_ARRAY(BSON("validAfter"
                                                                        << Timestamp(0, 1)
                                                                        << "shard" << dstShardName))
                                                     << "onCurrentShardSince" << Timestamp(0, 1))),
                         Timestamp(0)}));

            // Update "_id" in the document of the config.shards collection with srcShardName.
            // We can't update _id directly so we have to find the document, delete it,
            // update the BSONObj and re-insert.
            const auto docs = fassert(
                8291303,
                storageInterface->findDocuments(opCtx,
                                                NamespaceString::kConfigsvrShardsNamespace,
                                                IndexConstants::kIdIndexName,
                                                repl::StorageInterface::ScanDirection::kForward,
                                                BSON("_id" << srcShardName) /* startKey */,
                                                BoundInclusion::kIncludeStartKeyOnly,
                                                1 /* limit */));
            fassert(8291304, docs.size() == 1);

            fassert(8291305,
                    storageInterface->deleteById(opCtx,
                                                 NamespaceString::kConfigsvrShardsNamespace,
                                                 BSON("_id" << srcShardName).firstElement()));

            // See documentation of addFields, this replaces those 2 fields as they exist already.
            auto doc = docs[0].addFields(BSON("_id" << dstShardName << "host" << dstShardConnStr));

            // TODO SERVER-87581: confirm that this does not create an oplog entry.
            fassert(8291306,
                    storageInterface->insertDocument(opCtx,
                                                     NamespaceString::kConfigsvrShardsNamespace,
                                                     {doc, Timestamp(0)},
                                                     repl::OpTime::kUninitializedTerm));
        }

        // Update config.transaction_coordinators.
        auto status = storageInterface->updateDocuments(
            opCtx,
            NamespaceString::kTransactionCoordinatorsNamespace,
            {} /* query */,
            {BSON("$set" << BSON("participants.$[src]" << dstShardName)), Timestamp(0)},
            std::vector<BSONObj>{BSON("src" << srcShardName)} /* arrayFilters */);
        if (status != ErrorCodes::NamespaceNotFound) {
            fassert(8291401, status);
        }

        if (isConfig(restoreConfig)) {
            // Update config.reshardingOperations.
            fassert(8756801,
                    storageInterface->updateDocuments(
                        opCtx,
                        NamespaceString::kConfigReshardingOperationsNamespace,
                        {} /* query */,
                        {BSON("$set" << BSON("donorShards.$[src].id" << dstShardName
                                                                     << "recipientShards.$[src].id"
                                                                     << dstShardName)),
                         Timestamp(0)},
                        std::vector<BSONObj>{BSON("src.id" << srcShardName)} /* arrayFilters */));
            fassert(8756802,
                    storageInterface->updateDocuments(
                        opCtx,
                        NamespaceString::kConfigReshardingOperationsNamespace,
                        {BSON("state" << BSON("$ne"
                                              << "committing"))} /* query */,
                        {BSON("$set" << BSON("state"
                                             << "aborting"
                                             << "abortReason"
                                             << BSON("code" << ErrorCodes::ReshardCollectionAborted
                                                            << "errmsg"
                                                            << "aborted by automated restore"))),
                         Timestamp(0)}));
        }

        if (isShard(restoreConfig)) {
            // Update "donorShardId" in documents of the config.migrationCoordinators collection.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kMigrationCoordinatorsNamespace,
                BSON("donorShardId" << srcShardName),
                {BSON("$set" << BSON("donorShardId" << dstShardName)), Timestamp(0)});
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(8291402, status);
            }

            // Update "recipientShardId" in documents of the config.migrationCoordinators
            // collection.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kMigrationCoordinatorsNamespace,
                BSON("recipientShardId" << srcShardName),
                {BSON("$set" << BSON("recipientShardId" << dstShardName)), Timestamp(0)});
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(8291403, status);
            }

            // Update "donorShardId" in documents of the config.rangeDeletions
            // collection.
            fassert(8291404,
                    storageInterface->updateDocuments(
                        opCtx,
                        NamespaceString::kRangeDeletionNamespace,
                        BSON("donorShardId" << srcShardName),
                        {BSON("$set" << BSON("donorShardId" << dstShardName)), Timestamp(0)}));

            // Update the recipientShards array in config.localReshardingOperations.donor.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kDonorReshardingOperationsNamespace,
                {} /* query */,
                {BSON("$set" << BSON("recipientShards.$[src]" << dstShardName)), Timestamp(0)},
                std::vector<BSONObj>{BSON("src" << srcShardName)} /* arrayFilters */);
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(8291405, status);
            }

            // Update the donorShards array in config.localReshardingOperations.recipient.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kRecipientReshardingOperationsNamespace,
                {} /* query */,
                {BSON("$set" << BSON("donorShards.$[src]" << dstShardName)), Timestamp(0)},
                std::vector<BSONObj>{BSON("src" << srcShardName)} /* arrayFilters */);
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(8291406, status);
            }

            // Update the shardIds array in config.system.sharding_ddl_coordinators for
            // createCollection_V4 operations.
            // Note: shardIds is optional in create_collection_coordinator_document.idl so the
            // $exists below is to avoid "The path 'shardIds' must exist in the document in order to
            // apply array updates" errors.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kShardingDDLCoordinatorsNamespace,
                BSON("_id.operationType"
                     << "createCollection_V4"
                     << "shardIds" << BSON("$exists" << true)) /* query */,
                {BSON("$set" << BSON("shardIds.$[src]" << dstShardName)), Timestamp(0)},
                std::vector<BSONObj>{BSON("src" << srcShardName)} /* arrayFilters */);
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(8256801, status);
            }

            // Update the originalDataShard in config.system.sharding_ddl_coordinators for
            // createCollection_V4 operations.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kShardingDDLCoordinatorsNamespace,
                BSON("_id.operationType"
                     << "createCollection_V4"
                     << "originalDataShard" << srcShardName) /* query */,
                {BSON("$set" << BSON("originalDataShard" << dstShardName)), Timestamp(0)});
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(8256802, status);
            }

            // Update toShardId in config.system.sharding_ddl_coordinators for movePrimary
            // operations.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kShardingDDLCoordinatorsNamespace,
                BSON("_id.operationType"
                     << "movePrimary"
                     << "toShardId" << srcShardName),
                {BSON("$set" << BSON("toShardId" << dstShardName)), Timestamp(0)});
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(8256803, status);
            }
        }
    }
}

mongo::ShardIdentity getShardIdentity(OperationContext* opCtx,
                                      repl::StorageInterface* storageInterface) {
    // Find the shard identity document in admin.system.version.
    BSONObj shardIdentity =
        fassert(8291407,
                storageInterface->findById(opCtx,
                                           NamespaceString::kServerConfigurationNamespace,
                                           BSON("_id"
                                                << "shardIdentity")
                                               .firstElement()));

    // mongo::ShardIdentity::parse expects only the 3 fields below, removing others.
    auto fieldsToRemove = shardIdentity.getFieldNames<StringDataSet>();
    fieldsToRemove.erase("shardName");
    fieldsToRemove.erase("clusterId");
    fieldsToRemove.erase("configsvrConnectionString");
    shardIdentity = shardIdentity.removeFields(fieldsToRemove);
    return mongo::ShardIdentity::parse(IDLParserContext("RestoreConfiguration"), shardIdentity);
}

void setBalancerMode(OperationContext* opCtx, BalancerConfiguration* balancerConfig, bool stopped) {
    // TODO SERVER-89488: confirm this works as intended.
    fassert(8756803,
            balancerConfig->setBalancerMode(
                opCtx, stopped ? BalancerSettingsType::kOff : BalancerSettingsType::kFull));
}

// Create local.system.collections_to_restore.
void createCollectionsToRestore(
    OperationContext* opCtx,
    const std::vector<mongo::magic_restore::NamespaceUUIDPair>& nsAndUuids,
    repl::StorageInterface* storageInterface) {
    LOGV2(8290804, "Creating collections_to_restore collection for selective restore");
    fassert(8756805,
            storageInterface->createCollection(
                opCtx, NamespaceString::kConfigsvrRestoreNamespace, CollectionOptions()));

    // Insert all the names and UUIDs of the collections that were restored.
    // TODO SERVER-87581: confirm that this does not create an oplog entry.
    std::vector<InsertStatement> docs;
    docs.reserve(nsAndUuids.size());
    for (const auto& nsAndUuid : nsAndUuids) {
        docs.push_back(InsertStatement(
            BSON("_id" << OID::gen() << "ns"
                       << NamespaceStringUtil::serialize(nsAndUuid.getNs(),
                                                         SerializationContext::stateDefault())
                       << "uuid" << nsAndUuid.getUuid()),
            Timestamp(0),
            repl::OpTime::kUninitializedTerm));
    }
    fassert(8756806,
            storageInterface->insertDocuments(
                opCtx, NamespaceString::kConfigsvrRestoreNamespace, docs));
}

void updateShardingMetadata(OperationContext* opCtx,
                            const RestoreConfiguration& restoreConfig,
                            repl::StorageInterface* storageInterface) {
    LOGV2(8290805, "Updating sharding metadata");
    invariant(restoreConfig.getNodeType() != NodeTypeEnum::kReplicaSet);

    if (isConfig(restoreConfig)) {
        // Set the balancer state.
        if (restoreConfig.getBalancerSettings()) {
            checkInternalCollectionExists(opCtx, NamespaceString::kConfigSettingsNamespace);
            auto balancerConfig = Grid::get(opCtx)->getBalancerConfiguration();
            setBalancerMode(
                opCtx, balancerConfig, restoreConfig.getBalancerSettings()->getStopped());
        }

        // Drop config.mongos.
        LOGV2(9106006, "Dropping config.mongos");
        fassert(8756804,
                storageInterface->dropCollection(opCtx, NamespaceString::kConfigMongosNamespace));

        if (restoreConfig.getCollectionsToRestore()) {
            createCollectionsToRestore(
                opCtx, restoreConfig.getCollectionsToRestore().get(), storageInterface);

            // Run the _configsvrRunRestore command to clean up config metadata documents for
            // unrestored collections.
            DBDirectClient dbClient(opCtx);
            OpMsgRequest request;
            request.body = BSON("_configsvrRunRestore" << 1);
            LOGV2(8290806, "Running _configsvrRunRestore for selective restore");
            dbClient.runCommand(request);
            const auto commandReply = dbClient.runCommand(request)->getCommandReply();
            fassert(8756807, getStatusFromWriteCommandReply(commandReply));

            fassert(8756808,
                    storageInterface->dropCollection(opCtx,
                                                     NamespaceString::kConfigsvrRestoreNamespace));
        }
    }

    updateShardNameMetadata(opCtx, restoreConfig, storageInterface);

    mongo::ShardIdentity previousShardIdentity = getShardIdentity(opCtx, storageInterface);

    if (isConfig(restoreConfig)) {
        // Clear the shard identity document in admin.system.version.
        fassert(8291307,
                storageInterface->deleteById(opCtx,
                                             NamespaceString::kServerConfigurationNamespace,
                                             BSON("_id"
                                                  << "shardIdentity")
                                                 .firstElement()));
    }

    const auto& newShardIdentity = restoreConfig.getShardIdentityDocument();

    // validateRestoreConfiguration enforces this on the input. If we are renaming shards, the
    // shardIdentityDocument parameter must be passed in.
    invariant(newShardIdentity || !restoreConfig.getShardingRename());

    const ShardIdentity& shardIdentity =
        newShardIdentity ? *newShardIdentity : previousShardIdentity;

    // Update the shard identity document in admin.system.version.
    LOGV2(9106001, "Updating shard identity document");
    fassert(8291408,
            storageInterface->putSingleton(
                opCtx,
                NamespaceString::kServerConfigurationNamespace,
                BSON("_id"
                     << "shardIdentity"),
                {BSON("$set" << BSON("clusterId"
                                     << shardIdentity.getClusterId() << "shardName"
                                     << shardIdentity.getShardName() << "configsvrConnectionString"
                                     << shardIdentity.getConfigsvrConnectionString().toString())),
                 Timestamp(0)}));

    // Drop config.cache.collections.
    LOGV2(9106002, "Dropping config.cache.collections");
    fassert(
        8291409,
        storageInterface->dropCollection(opCtx, NamespaceString::kShardConfigCollectionsNamespace));

    // Drop "config.cache.chunks.*".
    LOGV2(9106003, "Dropping config.cache.chunks.*");
    fassert(
        8291410,
        storageInterface->dropCollectionsWithPrefix(opCtx, DatabaseName::kConfig, "cache.chunks."));

    // Drop config.cache.databases.
    LOGV2(9106004, "Dropping config.cache.databases");
    fassert(
        8291411,
        storageInterface->dropCollection(opCtx, NamespaceString::kShardConfigDatabasesNamespace));

    // Drop config.clusterParameters.
    LOGV2(9106005, "Dropping config.clusterParameters");
    fassert(8291412,
            storageInterface->dropCollection(opCtx, NamespaceString::kClusterParametersNamespace));
}

Timestamp insertHigherTermNoOpOplogEntry(OperationContext* opCtx,
                                         repl::StorageInterface* storageInterface,
                                         BSONObj& lastOplogEntry,
                                         long long higherTerm) {
    LOGV2(8290807, "Inserting no-op oplog entry with higher term value");
    const auto msgObj = BSON("msg"
                             << "restore incrementing term");

    const auto lastOplogEntryTs = lastOplogEntry["ts"].timestamp();
    const auto termToInsert = std::max(higherTerm, lastOplogEntry["t"].numberLong());

    // The new OpTime will have use the timestamp from the last oplog entry with its with epoch
    // seconds incremented by 1 and an increment set to 1. The chosen term is incremented by 100 as
    // additional buffer.
    const repl::OpTime opTime(
        repl::OpTime(Timestamp(lastOplogEntryTs.getSecs() + 1, 1), termToInsert + 100));

    repl::MutableOplogEntry oplogEntry;
    oplogEntry.setOpType(repl::OpTypeEnum::kNoop);
    oplogEntry.setNss({});
    oplogEntry.setObject(msgObj);
    oplogEntry.setOpTime(opTime);
    oplogEntry.setWallClockTime(lastOplogEntry["wall"].Date() + Seconds(1));

    writeConflictRetry(opCtx,
                       "Inserting higher term no-op oplog entry for magic restore",
                       NamespaceString::kRsOplogNamespace,
                       [&opCtx, &msgObj, &opTime, &oplogEntry] {
                           WriteUnitOfWork wuow(opCtx);
                           AutoGetOplogFastPath oplog(opCtx, OplogAccessMode::kWrite);
                           uassertStatusOK(collection_internal::insertDocument(
                               opCtx,
                               oplog.getCollection(),
                               InsertStatement{oplogEntry.toBSON()},
                               /*opDebug=*/nullptr));
                           wuow.commit();
                           // We need to move the oplog visibility forward for the no-op as some
                           // tests need to read the document.
                           const bool orderedCommit = true;
                           auto oplogRs = oplog.getCollection()->getRecordStore();
                           auto engine = opCtx->getServiceContext()->getStorageEngine();
                           uassertStatusOK(engine->oplogDiskLocRegister(
                               opCtx, oplogRs, opTime.getTimestamp(), orderedCommit));
                       });
    return opTime.getTimestamp();
}

ExitCode magicRestoreMain(ServiceContext* svcCtx) {
    BSONObjBuilder magicRestoreTimeElapsedBuilder;
    BSONObjBuilder magicRestoreInfoBuilder;

    Date_t beginMagicRestore = svcCtx->getFastClockSource()->now();

    LOGV2(8290800, "Beginning magic restore");
    auto opCtx = cc().makeOperationContext();

    LOGV2(8290608, "Reading magic restore configuration from stdin");

    auto reader = BSONStreamReader(std::cin);
    RestoreConfiguration restoreConfig;
    {
        TimeElapsedBuilderScopedTimer scopedTimer(svcCtx->getFastClockSource(),
                                                  "Reading magic restore configuration",
                                                  &magicRestoreTimeElapsedBuilder);
        restoreConfig =
            RestoreConfiguration::parse(IDLParserContext("RestoreConfiguration"), reader.getNext());
    }

    // Take unstable checkpoints from here on out. Nothing done as part of a restore is replication
    // rollback safe.
    LOGV2(8290809, "Setting initial data timestamp to the sentinel value");
    svcCtx->getStorageEngine()->setInitialDataTimestamp(
        Timestamp::kAllowUnstableCheckpointsSentinel);

    auto recoveryTimestamp = svcCtx->getStorageEngine()->getRecoveryTimestamp();
    // Stable checkpoint must exist.
    invariant(recoveryTimestamp);
    if (restoreConfig.getNodeType() == NodeTypeEnum::kReplicaSet &&
        recoveryTimestamp != restoreConfig.getMaxCheckpointTs()) {
        LOGV2_FATAL(8290700,
                    "For a replica set, the WiredTiger recovery timestamp from the data files must "
                    "match the maxCheckpointTs passed in via the restoreConfiguration.",
                    "recoveryTimestamp"_attr = recoveryTimestamp,
                    "maxCheckpointTs"_attr = restoreConfig.getMaxCheckpointTs());
    }

    // Truncates the oplog to the maxCheckpointTs value from the restore
    // configuration. This discards any journaled writes that exist in the restored data files from
    // beyond the checkpoint timestamp.
    LOGV2(8290810,
          "Truncating oplog to max checkpoint timestamp",
          "maxCheckPointTs"_attr = restoreConfig.getMaxCheckpointTs());
    auto replProcess = repl::ReplicationProcess::get(svcCtx);
    repl::StorageInterface* storageInterface;
    {
        TimeElapsedBuilderScopedTimer scopedTimer(
            svcCtx->getFastClockSource(),
            "Truncating oplog to max checkpoint timestamp and truncating local DB collections",
            &magicRestoreTimeElapsedBuilder);
        replProcess->getReplicationRecovery()->truncateOplogToTimestamp(
            opCtx.get(), restoreConfig.getMaxCheckpointTs());

        storageInterface = repl::StorageInterface::get(svcCtx);
        truncateLocalDbCollections(opCtx.get(), storageInterface);
    }

    LOGV2(8290811, "Inserting new replica set config");
    fassert(7197102,
            storageInterface->putSingleton(opCtx.get(),
                                           NamespaceString::kSystemReplSetNamespace,
                                           {restoreConfig.getReplicaSetConfig().toBSON()}));

    setInvalidMinValid(opCtx.get(), storageInterface);

    const auto pointInTimeTimestamp = restoreConfig.getPointInTimeTimestamp();
    if (pointInTimeTimestamp) {
        if (reader.hasNext()) {
            {
                TimeElapsedBuilderScopedTimer scopedTimer(
                    svcCtx->getFastClockSource(),
                    "Inserting additional entries into the oplog for a point-in-time restore",
                    &magicRestoreTimeElapsedBuilder);
                writeOplogEntriesToOplog(opCtx.get(), reader);
            }
            // For a PIT restore, we only want to insert oplog entries with timestamps up to and
            // including the pointInTimeTimestamp. External callers of magic restore should only
            // pass along entries up to the PIT timestamp, but we truncate the oplog after this
            // point to guarantee that we don't restore to a timestamp later than the PIT timestamp.
            LOGV2(8290812,
                  "Truncating oplog to PIT timestamp",
                  "pointInTimeTimestamp"_attr = pointInTimeTimestamp.get());
            {
                TimeElapsedBuilderScopedTimer scopedTimer(svcCtx->getFastClockSource(),
                                                          "Truncating oplog to PIT timestamp",
                                                          &magicRestoreTimeElapsedBuilder);
                replProcess->getReplicationRecovery()->truncateOplogToTimestamp(
                    opCtx.get(), pointInTimeTimestamp.get());
            }
            LOGV2(8290813, "Beginning to apply additional PIT oplog entries");
            {
                TimeElapsedBuilderScopedTimer scopedTimer(svcCtx->getFastClockSource(),
                                                          "Applying oplog entries for restore",
                                                          &magicRestoreTimeElapsedBuilder);
                replProcess->getReplicationRecovery()->applyOplogEntriesForRestore(
                    opCtx.get(), recoveryTimestamp.get());
            }
        } else {
            LOGV2(9035600, "No additional oplog entries to insert for PIT restore");
        }
    }

    if (restoreConfig.getSystemUuids()) {
        TimeElapsedBuilderScopedTimer scopedTimer(svcCtx->getFastClockSource(),
                                                  "Creating internal collections with UUID",
                                                  &magicRestoreTimeElapsedBuilder);
        createInternalCollectionsWithUuid(
            opCtx.get(), storageInterface, restoreConfig.getSystemUuids().get());
    }

    if (restoreConfig.getNodeType() != NodeTypeEnum::kReplicaSet) {
        TimeElapsedBuilderScopedTimer scopedTimer(svcCtx->getFastClockSource(),
                                                  "Updating sharding metadata",
                                                  &magicRestoreTimeElapsedBuilder);
        updateShardingMetadata(opCtx.get(), restoreConfig, storageInterface);
    } else {
        // TODO SERVER-91185: Test this logic in a targeted jstest.
        // The data files may be from an old shard node, and we want to restore a replica set node.
        // We should ensure the shard identity document has been removed.
        LOGV2(9106007, "Attempting to delete old shard identity document on replica set node");
        auto status = storageInterface->deleteById(opCtx.get(),
                                                   NamespaceString::kServerConfigurationNamespace,
                                                   BSON("_id"
                                                        << "shardIdentity")
                                                       .firstElement());
        if (status != ErrorCodes::NoSuchKey) {
            fassert(9106008, status);
        }
    }

    // Retrieve the timestamp of the last entry in the oplog, used for setting the initial data
    // timestamp below.
    BSONObj lastOplogEntryBSON;
    invariant(
        Helpers::getLast(opCtx.get(), NamespaceString::kRsOplogNamespace, lastOplogEntryBSON));
    Timestamp lastOplogEntryTs = lastOplogEntryBSON["ts"].timestamp();

    auto higherTerm = restoreConfig.getRestoreToHigherTermThan();
    if (higherTerm) {
        // If we've written a no-op entry, we must update the timestamp of the last entry in the
        // oplog.
        lastOplogEntryTs = insertHigherTermNoOpOplogEntry(
            opCtx.get(), storageInterface, lastOplogEntryBSON, higherTerm.get());

        // After inserting a new entry into the oplog, we need to update the stable timestamp. We'll
        // update the oldest timestamp again before exiting.
        LOGV2(8290814,
              "Setting stable timestamp to timestamp of higher term no-oplog entry",
              "ts"_attr = lastOplogEntryTs);
        opCtx->getServiceContext()->getStorageEngine()->setStableTimestamp(lastOplogEntryTs,
                                                                           false /*force*/);
    }

    auto autoCreds = restoreConfig.getAutomationCredentials();
    if (autoCreds) {
        TimeElapsedBuilderScopedTimer scopedTimer(svcCtx->getFastClockSource(),
                                                  "Upserting automation credentials",
                                                  &magicRestoreTimeElapsedBuilder);
        upsertAutomationCredentials(opCtx.get(), autoCreds.get(), storageInterface);
    }

    // Set the initial data timestamp to the stable timestamp. For a PIT restore, the stable
    // timestamp will be equal to the top of the oplog. This is the timestamp of either the latest
    // oplog entry streamed in via the restore configuration, or the no-op oplog entry with a higher
    // term. For a non-PIT restore, the stable timestamp will either be the checkpoint timestamp
    // from the snapshot, or the timestamp from the no-op oplog entry with a higher term from the
    // top of the oplog. By having a non-sentinel value initial data timestamp, we ensure WiredTiger
    // will take a stable checkpoint at shutdown.
    const auto stableTimestamp =
        opCtx->getServiceContext()->getStorageEngine()->getStableTimestamp();
    invariant(stableTimestamp == recoveryTimestamp.get() || stableTimestamp == lastOplogEntryTs);
    svcCtx->getStorageEngine()->setInitialDataTimestamp(stableTimestamp);
    // Set the oldest timestamp to the stable timestamp to discard any
    // history from the original snapshot.
    opCtx->getServiceContext()->getStorageEngine()->setOldestTimestamp(stableTimestamp, false);
    LOGV2(8290816, "Set stable and oldest timestamps", "ts"_attr = stableTimestamp);

    mongo::Milliseconds elapsedRestoreTime =
        svcCtx->getFastClockSource()->now() - beginMagicRestore;
    magicRestoreTimeElapsedBuilder.append("Magic Restore Tool total elapsed time",
                                          elapsedRestoreTime.toString());
    magicRestoreInfoBuilder.append("Statistics", magicRestoreTimeElapsedBuilder.obj());

    LOGV2(8290817,
          "Finished magic restore",
          "Summary of time elapsed"_attr = magicRestoreInfoBuilder.obj());
    exitCleanly(ExitCode::clean);
    return ExitCode::clean;
}

MONGO_STARTUP_OPTIONS_POST(MagicRestore)(InitializerContext*) {
    setMagicRestoreMain(magicRestoreMain);
}
}  // namespace magic_restore
}  // namespace mongo
