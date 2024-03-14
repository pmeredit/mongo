/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/bson/bsonobj.h"
#include "mongo/platform/basic.h"

#include "magic_restore.h"

#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "magic_restore/magic_restore_options_gen.h"
#include "magic_restore/magic_restore_structs_gen.h"
#include "mongo/bson/json.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_impl.h"
#include "mongo/db/catalog/collection_write_path.h"
#include "mongo/db/catalog/database_holder_impl.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/mongod_options.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/logv2/log.h"
#include "mongo/util/exit.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {
namespace magic_restore {

namespace moe = mongo::optionenvironment;


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
        LOGV2_FATAL_NOTRACE(8290500,
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
        LOGV2_FATAL_NOTRACE(
            8290501, "Parsed invalid BSON length in stream", "BSONLength"_attr = bsonLength);
    }
    const auto bytesToRead = bsonLength - _bsonLengthHeaderSizeBytes;
    _stream.read(_buffer.get() + _bsonLengthHeaderSizeBytes, bytesToRead);
    gcount = _stream.gcount();
    _totalBytesRead += gcount;
    if (!_stream || gcount < bytesToRead) {
        // We read a valid BSON object length, but the stream failed or we failed to read the
        // remainder of the object.
        LOGV2_FATAL_NOTRACE(8290502,
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

void copyFileDocumentsToOplog(ServiceContext* svcCtx, const std::string& filename) {
    int64_t bytesRead = 0;
    int64_t totalDocsInserted = 0;
    const auto bsonLengthHeaderSize = 4;
    const std::ios::openmode mode = std::ios::in | std::ios::binary;
    std::ifstream inputStream(filename, mode);
    if (!inputStream) {
        LOGV2_FATAL_NOTRACE(7197005,
                            "Failed to open the file of additional oplog entries",
                            "filename"_attr = filename,
                            "message"_attr = strerror(errno));
    }

    std::vector<char> buf;
    buf.reserve(1024 * 1024);

    auto opCtx = cc().makeOperationContext();
    while (true) {
        const auto objectOffset = bytesRead;

        inputStream.read(buf.data(), bsonLengthHeaderSize);
        if (inputStream.eof() && inputStream.gcount() == 0) {
            // We're done, there was no sign of error.
            return;
        }

        if (!inputStream && inputStream.gcount() > 0) {
            // If there's an error reading the 4-byte BSON length prefix, it must be because we
            // hit EOF. Propagate the error if we read even a single byte. The expectation is
            // that the input oplog entry file is corrupt.
            LOGV2_FATAL_NOTRACE(7197001,
                                "Failed to read bson object length prefix. Expected 4 bytes",
                                "received"_attr = inputStream.gcount(),
                                "offset"_attr = objectOffset);
        }
        bytesRead += inputStream.gcount();

        // The BSON length is always little endian.
        const std::int32_t bsonLength =
            ConstDataView(buf.data()).read<LittleEndian<std::int32_t>>();
        if (bsonLength < 0) {
            // Error out on negative length values that would fail the following `buf.reserve` and
            // I/O `read` calls. Otherwise let bogus bson data fail in the insert or oplog
            // application steps.
            LOGV2_FATAL_NOTRACE(7197002,
                                "Negative bson length",
                                "parsedLength"_attr = bsonLength,
                                "offset"_attr = objectOffset);
        }

        const auto bytesToRead = bsonLength - bsonLengthHeaderSize;
        buf.reserve(bsonLength);
        inputStream.read(buf.data() + bsonLengthHeaderSize, bytesToRead);
        bytesRead += inputStream.gcount();
        if (!inputStream && inputStream.gcount() < bytesToRead) {
            // We read a valid bson object length, but failed to read the remainder of the
            // object.
            LOGV2_FATAL_NOTRACE(7197003,
                                "Failed to read bson object",
                                "expectedSize"_attr = bsonLength,
                                "bytesRead"_attr = inputStream.gcount() + bsonLengthHeaderSize,
                                "offset"_attr = objectOffset);
        }

        writeConflictRetry(
            opCtx.get(), "Inserting into oplog", NamespaceString::kRsOplogNamespace, [&]() {
                WriteUnitOfWork wuow(opCtx.get());
                // AutoGetOplog relies on the LocalOplogInfo decoration being initialized. We're
                // writing these oplog entries before starting up replication. Use AutoGetCollection
                // directly on the oplog instead.
                AutoGetCollection oplog(
                    opCtx.get(), NamespaceString::kRsOplogNamespace, LockMode::MODE_IX);
                const BSONObj toInsert = BSONObj(buf.data(), BSONObj::LargeSizeTrait());
                uassertStatusOK(collection_internal::insertDocument(opCtx.get(),
                                                                    oplog.getCollection(),
                                                                    InsertStatement{toInsert},
                                                                    /*opDebug=*/nullptr));

                wuow.commit();

                // The system is running with the oplog visibility manager. However, because we're
                // in "standalone" mode, writes are not being timestamped. Force visibility forward
                // for each entry written to the oplog.
                const bool orderedCommit = true;
                uassertStatusOK(oplog->getRecordStore()->oplogDiskLocRegister(
                    opCtx.get(), toInsert["ts"].timestamp(), orderedCommit));
            });

        ++totalDocsInserted;
    }

    LOGV2(7197004,
          "All additional oplogs have been inserted",
          "totalDocsInserted"_attr = totalDocsInserted,
          "totalBytesInserted"_attr = bytesRead);
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
    Timestamp timestamp(0, 1);
    fassert(8291105,
            storageInterface->putSingleton(
                opCtx,
                NamespaceString::kDefaultMinValidNamespace,
                {BSON("_id" << OID() << "t" << -1 << "ts" << timestamp), timestamp}));
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
                                                StringData("_id_") /* indexName */,
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
            auto doc = docs[0].addFields(BSON("_id" << dstShardName << "hosts" << dstShardConnStr));

            // TODO SERVER-87581: confirm that this does not create an oplog entry.
            fassert(8291306,
                    storageInterface->insertDocument(opCtx,
                                                     NamespaceString::kConfigsvrShardsNamespace,
                                                     {doc, Timestamp(0)},
                                                     repl::OpTime::kUninitializedTerm));
        }

        // TODO SERVER-82914: Update config.transaction_coordinators

        if (isConfig(restoreConfig)) {
            /*
            TODO SERVER-87568: Update config.reshardingOperations
                For each object in the donorShards and recipientShard arrays:
                            {id: sourceShardName} -> {id: destShardName}
                If state != committing, set the state to "aborting" and set the abortReason to
               {code: ReshardCollectionAborted, errmsg: "aborted by automated restore"}
            */
        }

        if (isShard(restoreConfig)) {
            /* TODO SERVER-82914:
                (Shard) Update config.migrationCoordinators
                (Shard) Update config.rangeDeletions
                (Shard) Update config.localReshardingOperations
                (Shard) Update system.sharding_ddl_coordinators
            */
        }
    }
}

void updateShardingMetadata(OperationContext* opCtx,
                            const RestoreConfiguration& restoreConfig,
                            repl::StorageInterface* storageInterface) {
    invariant(restoreConfig.getNodeType() != NodeTypeEnum::kReplicaSet);

    if (isConfig(restoreConfig)) {
        /*
        TODO SERVER-87568: Set the balancer state
        TODO SERVER-87568: Drop the config.mongos collection
        TODO SERVER-87568: Update config collections for unrestored collections
        */
    }

    updateShardNameMetadata(opCtx, restoreConfig, storageInterface);

    if (isConfig(restoreConfig)) {
        // Clear the shard identity document.
        fassert(8291307,
                storageInterface->deleteById(opCtx,
                                             NamespaceString::kServerConfigurationNamespace,
                                             BSON("_id"
                                                  << "shardIdentity")
                                                 .firstElement()));
    }

    /*
    TODO SERVER-82914:
        Update shard identity document
        Drop config.cache.collections, config.cache.chunks.*, and config.cache.databases collections
        Drop the config.clusterParameters collection
    */
}

ExitCode magicRestoreMain(ServiceContext* svcCtx) {
    auto opCtx = cc().makeOperationContext();

    LOGV2(8290600, "Reading magic restore configuration from stdin");
    auto reader = BSONStreamReader(std::cin);
    auto restoreConfig =
        RestoreConfiguration::parse(IDLParserContext("RestoreConfiguration"), reader.getNext());

    // Take unstable checkpoints from here on out. Nothing done as part of a restore is replication
    // rollback safe.
    svcCtx->getStorageEngine()->setInitialDataTimestamp(
        Timestamp::kAllowUnstableCheckpointsSentinel);

    // Truncates the oplog to the maxCheckpointTs value from the restore configuration. This
    // discards any journaled writes that exist in the restored data files from beyond the
    // checkpoint timestamp.
    auto replProcess = repl::ReplicationProcess::get(svcCtx);
    replProcess->getReplicationRecovery()->truncateOplogToTimestamp(
        opCtx.get(), restoreConfig.getMaxCheckpointTs());

    auto* storageInterface = repl::StorageInterface::get(svcCtx);
    truncateLocalDbCollections(opCtx.get(), storageInterface);

    fassert(7197102,
            storageInterface->putSingleton(opCtx.get(),
                                           NamespaceString::kSystemReplSetNamespace,
                                           {restoreConfig.getReplicaSetConfig().toBSON()}));

    // For a PIT restore, we only want to insert oplog entries with timestamps up to and including
    // the pointInTimeTimestamp. External callers of magic restore should only pass along entries
    // up to the PIT timestamp, but we write this value to the truncate after point to guarantee
    // that we don't restore to a timestamp later than the PIT timestamp.
    if (auto pointInTimeTimestamp = restoreConfig.getPointInTimeTimestamp(); pointInTimeTimestamp) {
        replProcess->getConsistencyMarkers()->setOplogTruncateAfterPoint(
            opCtx.get(), pointInTimeTimestamp.get());
    }

    setInvalidMinValid(opCtx.get(), storageInterface);

    if (restoreConfig.getNodeType() != NodeTypeEnum::kReplicaSet) {
        updateShardingMetadata(opCtx.get(), restoreConfig, storageInterface);
    }

    exitCleanly(ExitCode::clean);
    return ExitCode::clean;
}

MONGO_STARTUP_OPTIONS_POST(MagicRestore)(InitializerContext*) {
    setMagicRestoreMain(magicRestoreMain);
}
}  // namespace magic_restore
}  // namespace mongo
