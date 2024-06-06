/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/filesystem.hpp>
#include <vector>

#include "mongo/base/string_data.h"

namespace mongo {
namespace repl {

/**
 * The Initial Sync File Mover performs the replacement of the storage files in the dbpath with
 * the backup storage files retrieved from the sync source.  It does this in a way that ensures
 * recovery (either to the previous state, an empty state, or a successful replacement) is
 * possible at any point.
 */
class InitialSyncFileMover {
public:
    static constexpr StringData kMovingFilesMarker = "INITIAL_SYNC_MOVING_FILES"_sd;
    static constexpr StringData kMovingFilesTmpMarker = "INITIAL_SYNC_MOVING_FILES.tmp"_sd;
    static constexpr StringData kFilesToDeleteMarker = "INITIAL_SYNC_FILES_TO_DELETE"_sd;
    static constexpr StringData kFilesToDeleteTmpMarker = "INITIAL_SYNC_FILES_TO_DELETE.tmp"_sd;
    static constexpr StringData kInitialSyncDir = ".initialsync"_sd;
    // These are copied from wiredtiger/src/include/log.h, which is internal to WiredTiger
    // and can't be included from here.
    static constexpr StringData kWiredTigerLogPrefix = "WiredTigerLog"_sd;
    static constexpr StringData kWiredTigerPreplogPrefix = "WiredTigerPreplog"_sd;
    static constexpr StringData kWiredTigerTmplogPrefix = "WiredTigerTmplog"_sd;

    // Used as a temporary directory where the storage engine switches inside while moving the
    // files from '.initialsync' to dbpath.
    static constexpr StringData kInitialSyncDummyDir = ".dummy"_sd;
    InitialSyncFileMover(const std::string& dbpath);

    /**
     * If a previous file based initial sync was in progress when the system was last shut down
     * or crashed, either complete it or clean it up.
     */
    void recoverFileCopyBasedInitialSyncAtStartup();

    /**
     * Deletes any list of files relative to dbpath.  As a special case, also deletes all
     * WiredTiger log files.
     */
    void deleteFiles(const std::vector<std::string>& filesToDelete);

    /**
     * Reads a list of filenames from the marker file.
     */
    std::vector<std::string> readListOfFiles(StringData markerName);

    /**
     * Creates the list of files to move by enumerating the files in the initial sync directory.
     * Then creates the kFilesToMoveMarker and deletes the kFilesToDeleteMarker.  As a convenience,
     * returns the list of files.
     *
     * This function skips any WiredTiger temp log and prep log files, as those would be deleted
     * by WiredTiger on startup anyway.
     */
    std::vector<std::string> createListOfFilesToMove();

    /**
     * Writes out a marker file, making sure the data is flushed before creating markerName and
     * making sure the directory is flushed afterwards.
     */
    void writeMarker(std::vector<std::string> filenames,
                     StringData markerName,
                     StringData tmpMarkerName);

    /**
     * Removes kFilesToMoveMarker and the empty kInitialSyncDir.
     */
    void completeMovingInitialSyncFiles();

    /*
     * Moves a list of files relative to kInitialSyncDir to dbpath.
     * If moving fails, deletes all the files in the kFilesToMove marker file and fasserts.
     */
    void moveFilesAndHandleFailure(const std::vector<std::string>& filesToMove);

    /**
     * Deletes the '.initialsync' directory relative to the dbpath.
     */
    static void deleteInitialSyncDir(std::string dbpath);

private:
    InitialSyncFileMover(const InitialSyncFileMover&) = delete;
    InitialSyncFileMover& operator=(const InitialSyncFileMover&) = delete;

    /**
     * Deletes the list of files in the kFilesToDeleteMarker file.
     */
    void _deleteFilesListedInDeleteMarker();

    /**
     * Deletes WiredTiger log files in the WiredTiger journal directory relative to the
     * passed-in dbpath.  We must delete the log files because it is possible another log file
     * got created after the list in the delete marker file was made.
     */
    void _deleteWiredTigerLogFiles(const boost::filesystem::path& dbpath);

    /**
     * Moves a list of files relative to kInitialSyncDir to dbpath.
     *
     * For most files, if the destination file exists and is non-empty, the file will be
     * postfixed a temporary name.  For WiredTiger log, tmp log, or prep log files,
     * an existing destination file will be removed, as WiredTiger cannot tolerate the
     * presence of files that have the same prefix as those files but not the exact name format
     * WiredTiger uses.
     */
    void _moveFiles(const std::vector<std::string>& filesToMove);


    /**
     * Renames the file from sourcePath to destinationPath, unless the files are on different
     * file systems in which case it copies the file to destinationPath and deletes sourcePath.
     */
    void _renameOrCopy(const boost::filesystem::path& sourcePath,
                       const boost::filesystem::path& destinationPath);

    /**
     * Returns true if the path is a file at the top level of a WiredTiger journal directory.
     * We treat WiredTiger log/preplog/tmplog files specially, and the criteria for a file to
     * be considered such is that it has an appropriate prefix, and that its parent directory
     * is called "journal".
     */
    bool _isRelativeFilePathInJournalDir(const boost::filesystem::path& path);

    /**
     * Returns true if the path is a WiredTiger log file, prep log file, or tmp log file.  Uses
     * prefix matching, because WiredTiger does also.
     */
    bool _isLogOrTmpLogFileRelativePath(const boost::filesystem::path& path);

    /**
     * Returns true if the path is a WiredTiger tmp log file (including prep logs).  Uses prefix
     * matching, because WiredTiger does also.
     */
    bool _isTmpLogFileRelativePath(const boost::filesystem::path& path);

    /**
     * Delete all files listed in the move marker, the .initialSync directory, and all markers,
     * and calls fassert().
     */
    MONGO_COMPILER_NORETURN void _cleanupAfterFailedMoveAndFassert();

    std::string _dbpath;
};

}  // namespace repl
}  // namespace mongo
