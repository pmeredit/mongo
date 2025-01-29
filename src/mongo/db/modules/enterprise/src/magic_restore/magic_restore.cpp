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
#include "mongo/db/server_lifecycle_monitor.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"
#include "mongo/s/balancer_configuration.h"
#include "mongo/s/grid.h"
#include "mongo/util/exit.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/string_map.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kMagicRestore

namespace mongo::magic_restore {

BSONStreamReader::BSONStreamReader(std::istream& stream) : _stream(stream) {
    _buffer = std::make_unique<char[]>(BSONObjMaxInternalSize);
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
    if (bsonLength < BSONObj::kMinBSONLength || bsonLength > BSONObjMaxInternalSize) {
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

const std::array<std::string, 9> approvedClusterParameters = {
    "defaultMaxTimeMS",
    "querySettings",
    "shardedClusterCardinalityForDirectConns",
    "fleCompactionOptions",
    "changeStreamOptions",
    "internalQueryCutoffForSampleFromRandomCursor",
    "internalSearchOptions",
    "configServerReadPreferenceForCatalogQueries",
    "pauseMigrationsDuringMultiUpdates"};

/**
 * Reads oplog entries from the BSONStreamReader and inserts them into the oplog. Each entry is
 * inserted in its own write unit of work. Note that the function will hold on to the global lock in
 * IX mode for the duration of oplog entry insertion.
 */
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


/**
 * Helper function to execute the automation agent credentials upsert. If a create command fails
 * with a DuplicateKey error, the function will convert the command into an update command run it
 * again.
 */
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

void renameLocalReshardingMetadataCollections(
    OperationContext* opCtx,
    repl::StorageInterface* storageInterface,
    const std::vector<std::string>& reshardingMetadataColls,
    const ShardId& srcShardName,
    const ShardId& dstShardName) {
    pcre::Regex re(".*" + srcShardName.toString() + "$");

    for (std::string name : reshardingMetadataColls) {
        fassert(8742900, name.starts_with("localResharding"));
        if (re.match(name)) {
            const auto fromNs = NamespaceString::makeGlobalConfigCollection(name);

            // Finds the position of the first character of the source shard ID, and then replaces
            // it with the destination shard ID.
            const auto pos = name.find(srcShardName.toString());
            name.replace(pos, srcShardName.toString().length(), dstShardName.toString());

            const auto toNs = NamespaceString::makeGlobalConfigCollection(name);
            LOGV2(8742901,
                  "Renaming resharding metadata collection",
                  "fromNs"_attr = fromNs,
                  "toNs"_attr = toNs);
            fassert(8742902,
                    storageInterface->renameCollection(opCtx, fromNs, toNs, false /* stayTemp */));
        }
    }
}

std::vector<std::string> getLocalReshardingMetadataColls(OperationContext* opCtx) {
    DBDirectClient client(opCtx);
    const auto filter = BSON(
        "type"
        << "collection"
        << "name"
        << BSON("$regex" << BSONRegEx("^(" + NamespaceString::kReshardingConflictStashPrefix + "|" +
                                      NamespaceString::kReshardingLocalOplogBufferPrefix + ")")));

    const auto collInfos = client.getCollectionInfos(DatabaseName::kConfig, filter);
    std::vector<std::string> collectionNames;
    std::transform(collInfos.begin(),
                   collInfos.end(),
                   std::back_inserter(collectionNames),
                   [](const BSONObj& coll) { return coll.getStringField("name").toString(); });

    return collectionNames;
}

void updateShardNameMetadata(OperationContext* opCtx,
                             const RestoreConfiguration& restoreConfig,
                             repl::StorageInterface* storageInterface) {
    const auto& shardingRename = restoreConfig.getShardingRename();

    if (!shardingRename) {
        return;
    }

    // These collections are renamed on shards.
    const auto reshardingMetadataColls = getLocalReshardingMetadataColls(opCtx);

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
            // Update shard names in config.reshardingOperations.
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
                {BSON("$set" << BSON("donorShards.$[src].shardId" << dstShardName)), Timestamp(0)},
                std::vector<BSONObj>{BSON("src.shardId" << srcShardName)} /* arrayFilters */);
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

            // If a movePrimary operation has entered the critical section, update fields in the
            // critical section document to reflect the new shard name.
            status = storageInterface->updateDocuments(
                opCtx,
                NamespaceString::kCollectionCriticalSectionsNamespace,
                BSON("reason.command"
                     << "movePrimary"
                     << "reason.to" << srcShardName),
                {BSON("$set" << BSON("reason.to" << dstShardName)), Timestamp(0)});
            if (status != ErrorCodes::NamespaceNotFound) {
                fassert(9031701, status);
            }

            renameLocalReshardingMetadataCollections(
                opCtx, storageInterface, reshardingMetadataColls, srcShardName, dstShardName);
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

void setBalancerSettingsStopped(OperationContext* opCtx,
                                repl::StorageInterface* storageInterface,
                                bool stopped) {
    LOGV2(8948800, "Setting 'stopped' field in balancer settings", "stopped"_attr = stopped);
    fassert(8756803,
            storageInterface->updateSingleton(opCtx,
                                              NamespaceString::kConfigSettingsNamespace,
                                              BSON("_id"
                                                   << "balancer"),
                                              {BSON("$set" << BSON("stopped" << stopped)),
                                               Timestamp(0)}));
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

void runSelectiveRestoreSteps(OperationContext* opCtx,
                              const RestoreConfiguration& restoreConfig,
                              repl::StorageInterface* storageInterface) {
    if (!storageGlobalParams.restore) {
        LOGV2_FATAL(8948400,
                    "Performing a selective restore with magic restore requires passing in "
                    "the --restore parameter.");
    }

    createCollectionsToRestore(
        opCtx, restoreConfig.getCollectionsToRestore().get(), storageInterface);

    // Run the _configsvrRunRestore command to clean up config metadata documents for
    // unrestored collections.
    DBDirectClient dbClient(opCtx);
    OpMsgRequest request;
    request.body = BSON("_configsvrRunRestore" << 1 << "$db"
                                               << "admin");
    LOGV2(8290806, "Running _configsvrRunRestore for selective restore");
    dbClient.runCommand(request);
    const auto commandReply = dbClient.runCommand(request)->getCommandReply();
    fassert(8756807, getStatusFromWriteCommandReply(commandReply));

    fassert(8756808,
            storageInterface->dropCollection(opCtx, NamespaceString::kConfigsvrRestoreNamespace));
}

void updateShardingMetadata(OperationContext* opCtx,
                            const RestoreConfiguration& restoreConfig,
                            repl::StorageInterface* storageInterface) {
    LOGV2(8290805, "Updating sharding metadata");
    invariant(restoreConfig.getNodeType() != NodeTypeEnum::kReplicaSet);

    if (isConfig(restoreConfig)) {
        // Set the balancer state if 'balancerSettings' exists on the restore configuration.
        if (restoreConfig.getBalancerSettings()) {
            checkInternalCollectionExists(opCtx, NamespaceString::kConfigSettingsNamespace);
            setBalancerSettingsStopped(
                opCtx, storageInterface, restoreConfig.getBalancerSettings()->getStopped());
        }

        // Drop config.mongos.
        LOGV2(9106006, "Dropping config.mongos");
        fassert(8756804,
                storageInterface->dropCollection(opCtx, NamespaceString::kConfigMongosNamespace));

        if (restoreConfig.getCollectionsToRestore()) {
            runSelectiveRestoreSteps(opCtx, restoreConfig, storageInterface);
        }
    }

    updateShardNameMetadata(opCtx, restoreConfig, storageInterface);

    mongo::ShardIdentity previousShardIdentity = getShardIdentity(opCtx, storageInterface);

    if (isConfig(restoreConfig)) {
        // Acquire the collection lock in IX mode, in case we have to perform updates.
        AutoGetCollection autoColl(
            opCtx, NamespaceString::kConfigReshardingOperationsNamespace, MODE_IX);
        if (autoColl.getCollection() && !autoColl->isEmpty(opCtx)) {
            // Set the resharding state to "aborting" for any in-progress resharding operations. We
            // do this regardless of if there is a shard rename.
            LOGV2(87429, "Aborting any in-progress resharding operations.");
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

        // Drop config.placementHistory.
        LOGV2(9322100, "Dropping config.placementHistory");
        fassert(9322101,
                storageInterface->dropCollection(
                    opCtx, NamespaceString::kConfigsvrPlacementHistoryNamespace));

        // Clear the shard identity document in admin.system.version.
        LOGV2(9322102, "Deleting shardIdentity document");
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
        storageInterface->dropCollection(opCtx, NamespaceString::kConfigCacheDatabasesNamespace));
}

void dropNonRestoredClusterParameters(OperationContext* opCtx,
                                      repl::StorageInterface* storageInterface) {
    // Drop config.clusterParameters.
    LOGV2(9106005, "Deleting all non-approved parameters from config.clusterParameters");
    BSONArrayBuilder builder;
    for (const auto& param : approvedClusterParameters) {
        builder << param;
    }
    auto filter = BSON("_id" << BSON("$nin" << builder.arr()));
    auto status = storageInterface->deleteByFilter(
        opCtx, NamespaceString::kClusterParametersNamespace, filter);
    if (status != ErrorCodes::NamespaceNotFound) {
        fassert(9195001, status);
    }
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

namespace {
/*
 * Runs the entire magic restore process. It reads the restore configuration from stdin, modifies
 * on-disk metadata to bring backed up data files to a consistent point-in-time. At the end of the
 * restore procedure, this function will signal a clean shutdown, and the process will exit.
 */
void entryPoint(ServiceContext* svcCtx) {
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

    // Drop non-approved cluster parameters on all nodes.
    dropNonRestoredClusterParameters(opCtx.get(), storageInterface);

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
    shutdownNoTerminate();
}

/**
 * Manages the thread that runs magic restore. The manager only executes restore logic
 * if '--magicRestore' is passed in as a startup parameter. Once completed, magic restore will
 * signal clean shutdown.
 */
class Manager {
public:
    /**
     * Starts magic restore on a separate thread if the required startup parameter is present.
     * Otherwise returns early.
     */
    void startMagicRestoreIfNeeded() {
        if (!storageGlobalParams.magicRestore) {
            return;
        }
        // The magic restore main function executes restore logic that amends backed up data
        // files on disk. At the conclusion of its operation, it signals clean shutdown.
        _thread = stdx::thread([this] {
            ServiceContext* serviceContext = getGlobalServiceContext();
            Client::initThread("magicRestore", serviceContext->getService());
            entryPoint(serviceContext);
        });
    }

    /**
     * Joins the magic restore thread if it was started.
     */
    ~Manager() {
        if (_thread.joinable()) {
            _thread.join();
        }
    }

private:
    /**
     * Executes the magic restore process.
     */
    stdx::thread _thread;
};

const auto managerDecoration = ServiceContext::declareDecoration<Manager>();
// Registers the magic restore initialization function with the ServerLifecycleMonitor. The monitor
// will start magic restore once mongod startup is complete.
bool lifecycleRegistrationDummy = [] {
    globalServerLifecycleMonitor().addFinishingStartupCallback([] {
        auto& sc = *getGlobalServiceContext();
        sc[managerDecoration].startMagicRestoreIfNeeded();
    });
    return true;
}();
}  // namespace
}  // namespace mongo::magic_restore
