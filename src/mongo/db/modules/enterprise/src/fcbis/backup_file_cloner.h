/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <memory>
#include <vector>

#include "mongo/db/repl/base_cloner.h"
#include "mongo/db/repl/initial_sync_base_cloner.h"
#include "mongo/db/repl/initial_sync_shared_data.h"
#include "mongo/db/repl/task_runner.h"
#include "mongo/util/progress_meter.h"

namespace mongo {
namespace repl {

class BackupFileCloner final : public InitialSyncBaseCloner {
public:
    struct Stats {
        std::string filePath;
        size_t fileSize;
        int extensionNumber;
        Date_t start;
        Date_t end;
        size_t receivedBatches{0};
        size_t writtenBatches{0};
        size_t bytesCopied{0};

        std::string toString() const;
        BSONObj toBSON() const;
        void append(BSONObjBuilder* builder) const;
    };

    /**
     * Type of function to schedule file system tasks with the executor.
     *
     * Used for testing only.
     */
    using ScheduleFsWorkFn = unique_function<StatusWith<executor::TaskExecutor::CallbackHandle>(
        executor::TaskExecutor::CallbackFn)>;

    /**
     * Constructor for backupFileCloner.
     *
     * remoteFileName: Path of file to copy on remote system.
     * remoteFileSize: Size of remote file in bytes, used for progress messages and stats only.
     * relativePath: Path of file relative to dbpath on the remote system, as a
     *               boost::filesystem::path generic path.
     * extensionNumber: Sequence number of the backup extension this file belongs to, 0 for a file
     *                  that is part of the original backup (rather than an extension.  Used for
     *                  stats only.
     */
    BackupFileCloner(const UUID& backupId,
                     const std::string& remoteFileName,
                     size_t remoteFileSize,
                     const std::string& relativePath,
                     int extensionNumber,
                     InitialSyncSharedData* sharedData,
                     const HostAndPort& source,
                     DBClientConnection* client,
                     StorageInterface* storageInterface,
                     ThreadPool* dbPool);

    ~BackupFileCloner() override = default;

    /**
     * Waits for any file system work to finish or fail.
     */
    void waitForFilesystemWorkToComplete();

    Stats getStats() const;

    std::string toString() const;

    /**
     * Overrides how executor schedules file system work.
     *
     * For testing only.
     */
    void setScheduleFsWorkFn_forTest(ScheduleFsWorkFn scheduleFsWorkFn) {
        _scheduleFsWorkFn = std::move(scheduleFsWorkFn);
    }

protected:
    ClonerStages getStages() final;

    bool isMyFailPoint(const BSONObj& data) const final;

private:
    friend class BackupFileClonerTest;

    class BackupFileClonerQueryStage : public ClonerStage<BackupFileCloner> {
    public:
        BackupFileClonerQueryStage(std::string name,
                                   BackupFileCloner* cloner,
                                   ClonerRunFn stageFunc)
            : ClonerStage<BackupFileCloner>(name, cloner, stageFunc) {}

        bool checkSyncSourceValidityOnRetry() override {
            // Sync source validity is assured by the backup ID not existing if the sync source
            // is restarted or otherwise becomes invalid.
            return false;
        }

        bool isTransientError(const Status& status) override {
            if (isCursorError(status)) {
                return true;
            }
            return ErrorCodes::isRetriableError(status);
        }

        static bool isCursorError(const Status& status) {
            // Our cursor was killed on the sync source.
            if ((status == ErrorCodes::CursorNotFound) || (status == ErrorCodes::OperationFailed) ||
                (status == ErrorCodes::QueryPlanKilled)) {
                return true;
            }
            return false;
        }
    };

    std::string describeForFuzzer(BaseClonerStage* stage) const final {
        // We do not have a fuzzer for file-based initial sync.
        MONGO_UNREACHABLE;
    }

    /**
     * The preStage sets the begin time in _stats and makes sure the destination file
     * can be created.
     */
    void preStage() final;

    /**
     * The postStage sets the end time in _stats.
     */
    void postStage() final;

    /**
     * Stage function that executes a query to retrieve the file data.  For each
     * batch returned by the upstream node, handleNextBatch will be called with the data.  This
     * stage will finish when the entire query is finished or failed.
     */
    AfterStageBehavior queryStage();

    /**
     * Put all results from a query batch into a buffer, and schedule it to be written to disk.
     */
    void handleNextBatch(DBClientCursor& cursor);

    /**
     * Called whenever there is a new batch of documents ready from the DBClientConnection.
     *
     * Each document returned will be inserted via the storage interfaceRequest storage
     * interface.
     */
    void writeDataToFilesystemCallback(const executor::TaskExecutor::CallbackArgs& cbd);

    /**
     * Sends an (aggregation) query command to the source. That query command with be parameterized
     * based on copy progress.
     */
    void runQuery();

    /**
     * Convenience call to get the file offset under a lock.
     */
    size_t getFileOffset();

    // All member variables are labeled with one of the following codes indicating the
    // synchronization rules for accessing them.
    //
    // (R)  Read-only in concurrent operation; no synchronization required.
    // (S)  Self-synchronizing; access according to class's own rules.
    // (M)  Reads and writes guarded by _mutex (defined in base class).
    // (X)  Access only allowed from the main flow of control called from run() or constructor.
    const UUID _backupId;                    // (R)
    const std::string _remoteFileName;       // (R)
    size_t _remoteFileSize;                  // (R)
    const std::string _relativePathString;   // (R)
    boost::filesystem::path _localFilePath;  // (X)

    BackupFileClonerQueryStage _queryStage;  // (R)

    std::ofstream _localFile;  // (M)
    // File offset we will request from the remote side in the next query.
    off_t _fileOffset = 0;  // (M)
    bool _sawEof = false;   // (X)

    ProgressMeter _progressMeter;  // (X) progress meter for this instance.
    //  Function for scheduling filesystem work using the executor.
    ScheduleFsWorkFn _scheduleFsWorkFn;  // (R)
    // Data read from source to insert.
    std::vector<BSONObj> _dataToWrite;  // (M)
    Stats _stats;                       // (M)
    // Putting _fsWorkTaskRunner last ensures anything the database work threads depend on
    // like _dataToWrite, is destroyed after those threads exit.
    TaskRunner _fsWorkTaskRunner;  // (R)

    static constexpr int kProgressMeterSecondsBetween = 60;
    static constexpr int kProgressMeterCheckInterval = 128;
};

}  // namespace repl
}  // namespace mongo
