/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "backup_file_cloner.h"

#include <fstream>

#include "initial_sync_file_mover.h"
#include "mongo/base/string_data.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplicationInitialSync

namespace mongo {
namespace repl {

// Failpoint which causes initial sync to hang after handling the next batch of results from the
// DBClientConnection, optionally limited to a specific file name.
MONGO_FAIL_POINT_DEFINE(initialSyncHangBackupFileClonerAfterHandlingBatchResponse);
MONGO_FAIL_POINT_DEFINE(initialSyncHangDuringBackupFileClone);
MONGO_FAIL_POINT_DEFINE(initialSyncBackupFileClonerDisableExhaust);
BackupFileCloner::BackupFileCloner(const UUID& backupId,
                                   const std::string& remoteFileName,
                                   size_t remoteFileSize,
                                   const std::string& relativePath,
                                   int extensionNumber,
                                   InitialSyncSharedData* sharedData,
                                   const HostAndPort& source,
                                   DBClientConnection* client,
                                   StorageInterface* storageInterface,
                                   ThreadPool* dbPool)
    : InitialSyncBaseCloner(
          "BackupFileCloner"_sd, sharedData, source, client, storageInterface, dbPool),
      _backupId(backupId),
      _remoteFileName(remoteFileName),
      _remoteFileSize(remoteFileSize),
      _relativePathString(relativePath),
      _queryStage("query", this, &BackupFileCloner::queryStage),
      _progressMeter(remoteFileSize,
                     kProgressMeterSecondsBetween,
                     kProgressMeterCheckInterval,
                     "bytes copied",
                     str::stream() << _remoteFileName << " backup file clone progress"),
      _scheduleFsWorkFn([this](executor::TaskExecutor::CallbackFn work) {
          auto task = [this, work = std::move(work)](
                          OperationContext* opCtx,
                          const Status& status) mutable noexcept -> TaskRunner::NextAction {
              try {
                  work(executor::TaskExecutor::CallbackArgs(nullptr, {}, status, opCtx));
              } catch (const DBException& e) {
                  setSyncFailedStatus(e.toStatus());
              }
              return TaskRunner::NextAction::kDisposeOperationContext;
          };
          _fsWorkTaskRunner.schedule(std::move(task));
          return executor::TaskExecutor::CallbackHandle();
      }),
      _fsWorkTaskRunner(dbPool) {
    _stats.filePath = _relativePathString;
    _stats.fileSize = _remoteFileSize;
    _stats.extensionNumber = extensionNumber;
}

BaseCloner::ClonerStages BackupFileCloner::getStages() {
    return {&_queryStage};
}


void BackupFileCloner::preStage() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _stats.start = getSharedData()->getClock()->now();

    // Construct local path name from the relative path and the local dbpath and initial sync dir.
    boost::filesystem::path relativePath(_relativePathString);
    uassert(5781700,
            str::stream() << "Path " << _relativePathString << " should be a relative path",
            relativePath.is_relative());
    boost::filesystem::path initialSyncPath(storageGlobalParams.dbpath);
    initialSyncPath.append(InitialSyncFileMover::kInitialSyncDir.toString());
    _localFilePath = initialSyncPath;

    _localFilePath /= relativePath;
    _localFilePath = _localFilePath.lexically_normal();
    uassert(
        5781701,
        str::stream() << "Path " << _relativePathString << " must not escape its parent directory.",
        StringData(_localFilePath.generic_string()).startsWith(initialSyncPath.generic_string()));

    // Create and open files and any parent directories.
    if (boost::filesystem::exists(_localFilePath)) {
        LOGV2(5781704,
              "Local file exists at start of BackupFileCloner; truncating.",
              "localFilePath"_attr = _localFilePath.string());
    } else {
        auto localFileDir = _localFilePath.parent_path();
        boost::system::error_code ec;
        boost::filesystem::create_directories(localFileDir, ec);
        uassert(5781703,
                str::stream() << "Failed to create directory " << localFileDir.string() << " Error "
                              << ec.message(),
                !ec);
    }
    _localFile.open(_localFilePath.string(),
                    std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    uassert(ErrorCodes::FileOpenFailed,
            str::stream() << "Failed to open file " << _localFilePath.string()
                          << " OS Error: " << errno << " msg " << strerror(errno),
            !_localFile.fail());
    _fileOffset = 0;
}

void BackupFileCloner::postStage() {
    _localFile.close();
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _stats.end = getSharedData()->getClock()->now();
}

BaseCloner::AfterStageBehavior BackupFileCloner::queryStage() {
    // Since the query stage may be re-started, we need to make sure all the file system work
    // from the previous run is done before running the query again.
    waitForFilesystemWorkToComplete();
    _sawEof = false;
    runQuery();
    waitForFilesystemWorkToComplete();
    uassert(
        5781710,
        str::stream()
            << "Received entire file, but did not get end of file marker.  File may be incomplete "
            << _localFilePath.string(),
        _sawEof);
    return kContinueNormally;
}

size_t BackupFileCloner::getFileOffset() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    return _fileOffset;
}

void BackupFileCloner::runQuery() {
    auto backupFileStage = BSON(
        "$_backupFile" << BSON("backupId" << _backupId << "file" << _remoteFileName << "byteOffset"
                                          << static_cast<int64_t>(getFileOffset())));
    AggregateCommandRequest aggRequest(
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin), {backupFileStage});
    aggRequest.setReadConcern(ReadConcernArgs::kLocal);
    aggRequest.setWriteConcern(WriteConcernOptions());

    LOGV2_DEBUG(5781702,
                2,
                "Backup file cloner running aggregation",
                "source"_attr = getSource(),
                "aggRequest"_attr = aggRequest.toBSON());
    const bool useExhaust = !MONGO_unlikely(initialSyncBackupFileClonerDisableExhaust.shouldFail());
    std::unique_ptr<DBClientCursor> cursor = uassertStatusOK(DBClientCursor::fromAggregationRequest(
        getClient(), std::move(aggRequest), true /* secondaryOk */, useExhaust));
    try {
        while (cursor->more()) {
            handleNextBatch(*cursor);
        }
    } catch (const DBException& e) {
        // We cannot continue after an error when processing exhaust cursors. Instead we must
        // reconnect, which is handled by the BaseCloner.
        LOGV2(5781715,
              "Backup file cloning received an exception while downloading data",
              "error"_attr = e.toStatus(),
              "source"_attr = getSource(),
              "backupId"_attr = _backupId,
              "remoteFile"_attr = _remoteFileName,
              "fileOffset"_attr = getFileOffset());
        getClient()->shutdown();
        throw;
    }
}

void BackupFileCloner::handleNextBatch(DBClientCursor& cursor) {
    LOGV2_DEBUG(5781712,
                3,
                "BackupFileCloner handleNextBatch",
                "source"_attr = getSource(),
                "backupId"_attr = _backupId,
                "remoteFile"_attr = _remoteFileName,
                "fileOffset"_attr = getFileOffset(),
                "moreInCurrentBatch"_attr = cursor.moreInCurrentBatch());
    {
        stdx::lock_guard<InitialSyncSharedData> lk(*getSharedData());
        if (!getSharedData()->getStatus(lk).isOK()) {
            static constexpr char message[] =
                "BackupFile cloning cancelled due to initial sync failure";
            LOGV2(5871706, message, "error"_attr = getSharedData()->getStatus(lk));
            uasserted(ErrorCodes::CallbackCanceled,
                      str::stream() << message << ": " << getSharedData()->getStatus(lk));
        }
    }
    while (cursor.moreInCurrentBatch()) {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        _stats.receivedBatches++;
        while (cursor.moreInCurrentBatch()) {
            _dataToWrite.emplace_back(cursor.nextSafe());
        }
    }

    // Schedule the next set of writes.
    auto&& scheduleResult =
        _scheduleFsWorkFn([=, this](const executor::TaskExecutor::CallbackArgs& cbd) {
            writeDataToFilesystemCallback(cbd);
        });

    if (!scheduleResult.isOK()) {
        Status newStatus = scheduleResult.getStatus().withContext(
            str::stream() << "Error copying file '" << _remoteFileName << "'");
        // We must throw an exception to terminate query.
        uassertStatusOK(newStatus);
    }

    initialSyncHangBackupFileClonerAfterHandlingBatchResponse.executeIf(
        [&](const BSONObj&) {
            while (MONGO_unlikely(
                       initialSyncHangBackupFileClonerAfterHandlingBatchResponse.shouldFail()) &&
                   !mustExit()) {
                LOGV2(5781711,
                      "initialSyncHangBackupFileClonerAfterHandlingBatchResponse fail point "
                      "enabled. Blocking until fail point is disabled",
                      "remoteFile"_attr = _remoteFileName);
                mongo::sleepmillis(100);
            }
        },
        [&](const BSONObj& data) {
            // Only hang when copying the specified file, or if no file was specified.
            auto filename = data["remoteFile"].str();
            return filename.empty() || filename == _remoteFileName;
        });
}

void BackupFileCloner::writeDataToFilesystemCallback(
    const executor::TaskExecutor::CallbackArgs& cbd) {
    LOGV2_DEBUG(5781713,
                3,
                "BackupFileCloner writeDataToFilesystemCallback",
                "backupId"_attr = _backupId,
                "remoteFile"_attr = _remoteFileName,
                "localFile"_attr = _localFilePath.string(),
                "fileOffset"_attr = getFileOffset(),
                "error"_attr = cbd.status);
    uassertStatusOK(cbd.status);
    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        if (!getStatus(lk).isOK()) {
            // If we've already failed, must not continue running here, because we'll
            // probably invariant if we do.
            return;
        }
        if (_dataToWrite.size() == 0) {
            LOGV2_WARNING(5781707,
                          "writeDataToFilesystemCallback, but no data to write",
                          "remoteFile"_attr = _remoteFileName);
        }
        try {
            for (const auto& doc : _dataToWrite) {
                uassert(5781714,
                        str::stream()
                            << "Saw multiple end-of-file-markers in file " << _remoteFileName,
                        !_sawEof);
                // Received file data should always be in sync with the stream and where we think
                // our next input should be coming from.
                const auto byteOffset = doc["byteOffset"].safeNumberLong();
                invariant(byteOffset == _localFile.tellp());
                invariant(byteOffset == _fileOffset);
                const auto& dataElem = doc["data"];
                uassert(5781708,
                        str::stream() << "Expected file data to be type BinDataGeneral. " << doc,
                        dataElem.type() == BinData && dataElem.binDataType() == BinDataGeneral);
                int dataLength;
                auto data = dataElem.binData(dataLength);
                _localFile.write(data, dataLength);
                uassert(ErrorCodes::FileStreamFailed,
                        str::stream() << "Unable to write file data for file " << _remoteFileName
                                      << " at offset " << _fileOffset << " OS Error: " << errno
                                      << " msg " << strerror(errno),
                        !_localFile.fail());
                _progressMeter.hit(dataLength);
                _fileOffset += dataLength;
                _stats.bytesCopied += dataLength;
                _sawEof = doc["endOfFile"].booleanSafe();
            }
            _dataToWrite.clear();
            _stats.writtenBatches++;
        } catch (const DBException& e) {
            // We need to set the status here before we release the lock, otherwise we
            // might execute the next callback after an error has occurred.
            LOGV2(9085500, "BackupFileCloner filesystem exception", "error"_attr = e.toStatus());
            setStatus(lk, e.toStatus());
            throw;
        }
    }

    initialSyncHangDuringBackupFileClone.executeIf(
        [&](const BSONObj&) {
            LOGV2(5781709,
                  "initial sync - initialSyncHangDuringBackupFileClone fail point "
                  "enabled. Blocking until fail point is disabled");
            while (MONGO_unlikely(initialSyncHangDuringBackupFileClone.shouldFail()) &&
                   !mustExit()) {
                mongo::sleepmillis(100);
            }
        },
        [&](const BSONObj& data) {
            return (data["remoteFile"].eoo() || data["remoteFile"].str() == _remoteFileName) &&
                (data["fileOffset"].eoo() || data["fileOffset"].safeNumberLong() <= _fileOffset);
        });
}

bool BackupFileCloner::isMyFailPoint(const BSONObj& data) const {
    auto remoteFile = data["remoteFile"].str();
    return (remoteFile.empty() || remoteFile == _remoteFileName) && BaseCloner::isMyFailPoint(data);
}

void BackupFileCloner::waitForFilesystemWorkToComplete() {
    _fsWorkTaskRunner.join();
    // We may have failed in the file system work.
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    uassertStatusOK(getStatus(lk));
}

BackupFileCloner::Stats BackupFileCloner::getStats() const {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    return _stats;
}

std::string BackupFileCloner::Stats::toString() const {
    return toBSON().toString();
}


BSONObj BackupFileCloner::Stats::toBSON() const {
    BSONObjBuilder bob;
    append(&bob);
    return bob.obj();
}

void BackupFileCloner::Stats::append(BSONObjBuilder* builder) const {
    builder->append("filePath", filePath);
    builder->appendNumber("fileSize", static_cast<long long>(fileSize));
    builder->appendNumber("bytesCopied", static_cast<long long>(bytesCopied));
    if (extensionNumber > 0)
        builder->appendNumber("extensionNumber", static_cast<long long>(extensionNumber));
    if (start != Date_t()) {
        builder->appendDate("start", start);
        if (end != Date_t()) {
            builder->appendDate("end", end);
            auto elapsed = end - start;
            long long elapsedMillis = duration_cast<Milliseconds>(elapsed).count();
            builder->appendNumber("elapsedMillis", elapsedMillis);
        }
    }
    builder->appendNumber("receivedBatches", static_cast<long long>(receivedBatches));
    builder->appendNumber("writtenBatches", static_cast<long long>(writtenBatches));
}

}  // namespace repl
}  // namespace mongo
