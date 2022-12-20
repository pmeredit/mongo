/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "magic_restore.h"

#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <memory>

#include "mongo/bson/json.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_impl.h"
#include "mongo/db/catalog/collection_write_path.h"
#include "mongo/db/catalog/database_holder_impl.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/modules/enterprise/src/magic_restore/magic_restore_options_gen.h"
#include "mongo/db/modules/enterprise/src/magic_restore/magic_restore_structs_gen.h"
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

namespace moe = mongo::optionenvironment;

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
            opCtx.get(), "Inserting into oplog", NamespaceString::kRsOplogNamespace.ns(), [&]() {
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

ExitCode magicRestoreMain(ServiceContext* svcCtx) {
    BSONObj restoreConfigObj;
    auto opCtx = cc().makeOperationContext();

    // An empty vector implies all collections are to be restored.
    std::vector<CollectionToRestore> collectionAllowList;
    std::vector<ShardRenameMapping> shardRenameMappings;

    moe::Environment& params = moe::startupOptionsParsed;
    if (params.count("restoreConfiguration")) {
        const std::string restoreConfigFilename = params["restoreConfiguration"].as<std::string>();

        std::string configContents;
        Status status =
            moe::readRawFile(restoreConfigFilename, &configContents, moe::ConfigExpand{});
        restoreConfigObj = fromjson(configContents);
    }

    std::string oplogEntriesFilename;
    if (params.count("additionalOplogEntriesFile")) {
        oplogEntriesFilename = params["additionalOplogEntriesFile"].as<std::string>();
    }

    if (params.count("additionalOplogEntriesViaStdIn")) {
    }

    // Take unstable checkpoints from here on out. Nothing done as part of a restore is replication
    // rollback safe.
    svcCtx->getStorageEngine()->setInitialDataTimestamp(
        Timestamp::kAllowUnstableCheckpointsSentinel);

    auto replProcess = repl::ReplicationProcess::get(svcCtx);
    BSONElement pointInTimeTimestamp = restoreConfigObj["pointInTimeTimestamp"];
    if (pointInTimeTimestamp.ok()) {
        fassertNoTrace(7190501, pointInTimeTimestamp.type() == bsonTimestamp);
        replProcess->getConsistencyMarkers()->setOplogTruncateAfterPoint(
            opCtx.get(), pointInTimeTimestamp.timestamp());
    }

    auto* storageInterface = repl::StorageInterface::get(svcCtx);
    fassert(7197101,
            storageInterface->truncateCollection(opCtx.get(),
                                                 NamespaceString::kSystemReplSetNamespace));
    fassert(7197102,
            storageInterface->putSingleton(opCtx.get(),
                                           NamespaceString::kSystemReplSetNamespace,
                                           {restoreConfigObj["replicaSetConfig"].Obj()}));

    if (oplogEntriesFilename.size()) {
        copyFileDocumentsToOplog(svcCtx, oplogEntriesFilename);
    }

    if (const BSONElement& collectionsToRestore = restoreConfigObj["collectionsToRestore"];
        !collectionsToRestore.eoo()) {
        IDLParserContext idlCtx("collectionsToRestoreParser");
        for (const BSONElement& elem : collectionsToRestore.Obj()) {
            collectionAllowList.emplace_back(CollectionToRestore::parse(idlCtx, elem.Obj()));
        }
    }

    if (const BSONElement& shardingRename = restoreConfigObj["shardingRename"];
        !shardingRename.eoo()) {
        IDLParserContext idlCtx("shardingRenameParser");
        for (const BSONElement& elem : shardingRename.Obj()) {
            shardRenameMappings.emplace_back(ShardRenameMapping::parse(idlCtx, elem.Obj()));
        }
    }

    exitCleanly(ExitCode::clean);
    return ExitCode::clean;
}

MONGO_STARTUP_OPTIONS_POST(MagicRestore)(InitializerContext*) {
    setMagicRestoreMain(magicRestoreMain);
}
}  // namespace mongo
