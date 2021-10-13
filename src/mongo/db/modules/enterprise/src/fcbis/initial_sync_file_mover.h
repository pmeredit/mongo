/**
 *    Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
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
     * Deletes any list of files relative to dbpath.
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

private:
    InitialSyncFileMover(const InitialSyncFileMover&) = delete;
    InitialSyncFileMover& operator=(const InitialSyncFileMover&) = delete;

    /**
     * Deletes the list of files in the kFilesToDeleteMarker file.
     */
    void _deleteFilesListedInDeleteMarker();

    /**
     * Moves a list of files relative to kInitialSyncDir to dbpath.
     */
    void _moveFiles(const std::vector<std::string>& filesToMove);


    /**
     * Delete all files listed in the move marker, the .initialSync directory, and all markers,
     * and calls fassert().
     */
    MONGO_COMPILER_NORETURN void _cleanupAfterFailedMoveAndFassert();

    std::string _dbpath;
};

}  // namespace repl
}  // namespace mongo
