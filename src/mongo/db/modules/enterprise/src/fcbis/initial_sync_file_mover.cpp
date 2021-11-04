/**
 *    Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include "initial_sync_file_mover.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <stack>

#include "mongo/db/storage/storage_file_util.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"


namespace mongo {
namespace repl {

InitialSyncFileMover::InitialSyncFileMover(const std::string& dbpath) : _dbpath(dbpath) {}

void InitialSyncFileMover::recoverFileCopyBasedInitialSyncAtStartup() {
    boost::filesystem::path dbpath(_dbpath);
    auto initialSyncDir = dbpath;
    initialSyncDir.append(kInitialSyncDir.toString());
    if (!boost::filesystem::exists(initialSyncDir)) {
        LOGV2_DEBUG(5783400,
                    3,
                    "No file copy based initial sync was in progress, so no recovery is necessary",
                    "dbpath"_attr = dbpath.string(),
                    "initialSyncDir"_attr = initialSyncDir.string());
        return;
    }
    auto filesToDeleteTmpMarker = dbpath;
    filesToDeleteTmpMarker.append(kFilesToDeleteTmpMarker.toString());
    auto filesToDeleteMarker = dbpath;
    filesToDeleteMarker.append(kFilesToDeleteMarker.toString());
    auto movingFilesTmpMarker = dbpath;
    movingFilesTmpMarker.append(kMovingFilesTmpMarker.toString());
    auto movingFilesMarker = dbpath;
    movingFilesMarker.append(kMovingFilesMarker.toString());

    // The temporary markers should always be deleted.
    boost::filesystem::remove(movingFilesTmpMarker);
    boost::filesystem::remove(filesToDeleteTmpMarker);

    bool moveMarkerExists = boost::filesystem::exists(movingFilesMarker);
    bool deleteMarkerExists = boost::filesystem::exists(filesToDeleteMarker);
    if (!moveMarkerExists && !deleteMarkerExists) {
        LOGV2_WARNING(5783401,
                      "A previous file based initial sync was in progress when the server stopped, "
                      "but it cannot be completed.",
                      "dbpath"_attr = dbpath.string(),
                      "initialSyncDir"_attr = initialSyncDir.string());
        boost::filesystem::remove_all(initialSyncDir);
        return;
    }

    LOGV2(5783402,
          "A previous file based initial sync was in progress when the server stopped.  Attempting "
          "to complete it.",
          "dbpath"_attr = dbpath.string(),
          "initialSyncDir"_attr = initialSyncDir.string());

    std::vector<std::string> filesToMove;
    if (!moveMarkerExists) {
        _deleteFilesListedInDeleteMarker();
        LOGV2_DEBUG(5783406, 1, "Recovering file based initial sync: old files have been deleted.");
        filesToMove = createListOfFilesToMove();
        writeMarker(filesToMove, kMovingFilesMarker, kMovingFilesTmpMarker);
    } else {
        filesToMove = readListOfFiles(kMovingFilesMarker);
    }
    if (deleteMarkerExists) {
        boost::filesystem::remove(filesToDeleteMarker);
    }
    moveFilesAndHandleFailure(filesToMove);
    LOGV2_DEBUG(5783407, 1, "Recovering file based initial sync: new files have been moved.");
    completeMovingInitialSyncFiles();
}

void InitialSyncFileMover::completeMovingInitialSyncFiles() {
    auto movingFilesMarker = boost::filesystem::path(_dbpath);
    movingFilesMarker.append(kMovingFilesMarker.toString());
    boost::filesystem::remove(movingFilesMarker);
    deleteInitialSyncDir(_dbpath);
    LOGV2(5783405, "File based initial sync has been completed.");
}

std::vector<std::string> InitialSyncFileMover::readListOfFiles(StringData markerName) {
    boost::filesystem::path path(_dbpath);
    path.append(markerName.toString());
    // The format of the marker files is simply strings of relative paths terminated with NUL
    // characters.  This avoids having to worry about the BSON 16MB limit.
    std::vector<std::string> result;
    auto file_size = boost::filesystem::file_size(path);
    std::vector<char> contents(file_size);
    std::ifstream reader(path.native(), std::ios_base::in | std::ios_base::binary);
    reader.read(contents.data(), file_size);
    auto startiter = contents.begin();
    auto nuliter = std::find(startiter, contents.end(), '\0');
    while (nuliter != contents.end()) {
        auto& nextFileName = result.emplace_back();
        std::copy(startiter, nuliter, std::back_inserter(nextFileName));
        startiter = nuliter + 1;
        nuliter = std::find(startiter, contents.end(), '\0');
    }
    return result;
}

std::vector<std::string> InitialSyncFileMover::createListOfFilesToMove() {
    std::vector<std::string> result;
    boost::filesystem::path initialSyncDir(_dbpath);
    std::stack<boost::filesystem::path> pathsToMove;
    initialSyncDir.append(kInitialSyncDir.toString());
    pathsToMove.push(initialSyncDir);
    while (!pathsToMove.empty()) {
        const auto pathToMove = pathsToMove.top();
        pathsToMove.pop();
        for (auto dirIter = boost::filesystem::directory_iterator(pathToMove);
             dirIter != boost::filesystem::directory_iterator();
             dirIter++) {
            auto fileToMove = boost::filesystem::relative(dirIter->path(), initialSyncDir);
            if (fileToMove.string() == kInitialSyncDummyDir) {
                // Skip the dummy directory.
                continue;
            }
            // For directories to be moved to symlinks, we must move the files in the directory,
            // not the symlink itself.
            boost::filesystem::path destinationPath(_dbpath);
            destinationPath /= fileToMove;
            if (boost::filesystem::is_symlink(destinationPath) &&
                boost::filesystem::is_directory(dirIter->path())) {
                pathsToMove.push(dirIter->path());
            } else {
                result.emplace_back(fileToMove.string());
            }
        }
    }
    return result;
}

void InitialSyncFileMover::writeMarker(std::vector<std::string> filenames,
                                       StringData markerName,
                                       StringData tmpMarkerName) {
    boost::filesystem::path markerPath(_dbpath);
    markerPath.append(markerName.toString());
    boost::filesystem::path tmpMarkerPath(_dbpath);
    tmpMarkerPath.append(tmpMarkerName.toString());
    std::ofstream writer(tmpMarkerPath.native(), std::ios_base::out | std::ios_base::binary);
    for (const auto& filename : filenames) {
        writer.write(filename.c_str(), filename.size() + 1);
    }
    writer.close();
    fassertNoTrace(5783418, fsyncFile(tmpMarkerPath));
    boost::filesystem::rename(tmpMarkerPath, markerPath);
    fassertNoTrace(5783419, fsyncParentDirectory(markerPath));
}

void InitialSyncFileMover::_deleteFilesListedInDeleteMarker() {
    deleteFiles(readListOfFiles(kFilesToDeleteMarker));
}

void InitialSyncFileMover::deleteFiles(const std::vector<std::string>& filesToDelete) {
    boost::filesystem::path dbpath(_dbpath);
    dbpath = boost::filesystem::canonical(dbpath);
    StringSet removedFiles;
    StringSet symlinks;
    for (const auto& filename : filesToDelete) {
        boost::filesystem::path fileRelativePath(filename);
        // Marker files must contain only relative paths
        fassert(5783403, fileRelativePath.is_relative());
        // If the file list contains files in directories, we delete the whole directory.
        // This covers log files as well as directory-per-db and directory-per-index.
        // However, symlinks to directories are not considered directories.
        auto pathIter = fileRelativePath.begin();
        auto topPath = *pathIter;
        auto fullPath = dbpath;
        fullPath /= topPath;
        ++pathIter;
        while (pathIter != fileRelativePath.end() &&
               removedFiles.find(topPath.string()) == removedFiles.end() &&
               ((symlinks.find(topPath.string()) != symlinks.end()) ||
                boost::filesystem::is_symlink(fullPath))) {
            symlinks.insert(topPath.string());
            topPath /= *pathIter;
            fullPath /= *pathIter;
            ++pathIter;
        }
        // Avoid trying to remove the same file multiple times during the same run.  This avoids
        // log spam and excessive stats of the filesystem.
        if (removedFiles.find(topPath.string()) != removedFiles.end())
            continue;
        // Because we allow symlinks, we must only normalize, not canonicalize, this path.
        fullPath = fullPath.lexically_normal();
        // Paths must be relative to dbpath.
        auto [dbpath_mismatch, fullPath_mismatch] =
            std::mismatch(dbpath.begin(), dbpath.end(), fullPath.begin(), fullPath.end());
        if (dbpath_mismatch != dbpath.end()) {
            LOGV2_FATAL_NOTRACE(5783410,
                                "A file to be deleted as part of initial sync was not in the "
                                "dbpath or a subdirectory thereof.",
                                "dbpath"_attr = dbpath.string(),
                                "fullPath"_attr = fullPath.string());
        }
        auto file_status = boost::filesystem::status(fullPath);
        if (!boost::filesystem::exists(file_status)) {
            LOGV2_DEBUG(5783404,
                        2,
                        "Not deleting because file or directory does not exist",
                        "filename"_attr = filename,
                        "fullPath"_attr = fullPath.string());
        } else if (boost::filesystem::is_directory(file_status)) {
            LOGV2_DEBUG(5783408,
                        2,
                        "Deleting directory",
                        "filename"_attr = filename,
                        "fullPath"_attr = fullPath.string());
            boost::filesystem::remove_all(fullPath);
        } else {
            LOGV2_DEBUG(5783409,
                        2,
                        "Deleting file",
                        "filename"_attr = filename,
                        "fullPath"_attr = fullPath.string());
            boost::filesystem::remove(fullPath);
        }
        removedFiles.insert(topPath.string());
    }
}

void InitialSyncFileMover::_moveFiles(const std::vector<std::string>& filesToMove) {
    boost::filesystem::path dbpath(_dbpath);
    dbpath = boost::filesystem::canonical(dbpath);
    boost::filesystem::path initialSyncDir(dbpath);
    initialSyncDir.append(kInitialSyncDir.toString());
    StringSet removedFiles;
    for (const auto& filename : filesToMove) {
        boost::filesystem::path fileRelativePath(filename);
        // Marker files must contain only relative paths
        fassert(5783411, fileRelativePath.is_relative());
        auto fullSourcePath = initialSyncDir;
        fullSourcePath /= fileRelativePath;
        auto destinationPath = dbpath;
        destinationPath /= fileRelativePath;
        if (!boost::filesystem::exists(fullSourcePath)) {
            if (!boost::filesystem::exists(destinationPath)) {
                LOGV2_FATAL_CONTINUE(5783417,
                                     "A file to be moved does not exist in either the source or "
                                     "the destination directory. This is unrecoverable; the node "
                                     "must be resynced from the beginning.",
                                     "filename"_attr = filename,
                                     "fullSourcePath"_attr = fullSourcePath.string(),
                                     "destinationPath"_attr = destinationPath.string());
                _cleanupAfterFailedMoveAndFassert();
            }
            LOGV2_DEBUG(5783414,
                        2,
                        "A file to be moved does not exist.  It may have been moved previously",
                        "filename"_attr = filename,
                        "fullSourcePath"_attr = fullSourcePath.string(),
                        "destinationPath"_attr = destinationPath.string());
            continue;
        }
        fullSourcePath = boost::filesystem::canonical(fullSourcePath);
        // Paths must be relative to initialSyncDir.
        auto [initialSyncDir_mismatch, fullSourcePath_mismatch] =
            std::mismatch(initialSyncDir.begin(),
                          initialSyncDir.end(),
                          fullSourcePath.begin(),
                          fullSourcePath.end());
        if (initialSyncDir_mismatch != initialSyncDir.end()) {
            LOGV2_FATAL_NOTRACE(
                5783412,
                "A file to be moved as part of initial sync was not in the initialSyncDir.",
                "initialSyncDir"_attr = initialSyncDir.string(),
                "fullSourcePath"_attr = fullSourcePath.string());
        }
        if (boost::filesystem::exists(destinationPath)) {
            if (!boost::filesystem::is_empty(destinationPath)) {
                auto replacementPath = destinationPath;
                replacementPath += boost::filesystem::unique_path("-%%%%-%%%%-%%%%-%%%%");
                LOGV2_WARNING(5783413,
                              "A file to be moved as part of initial sync already exists, renaming "
                              "the old file",
                              "filename"_attr = filename,
                              "fullSourcePath"_attr = fullSourcePath.string(),
                              "destinationPath"_attr = destinationPath.string(),
                              "replacementPath"_attr = replacementPath.string());
                boost::filesystem::rename(destinationPath, replacementPath);
            } else {
                boost::filesystem::remove(destinationPath);
            }
        }
        boost::filesystem::rename(fullSourcePath, destinationPath);
    }
}

void InitialSyncFileMover::_cleanupAfterFailedMoveAndFassert() {
    deleteInitialSyncDir(_dbpath);
    auto movingFilesMarker = boost::filesystem::path(_dbpath);
    movingFilesMarker.append(kMovingFilesMarker.toString());
    deleteFiles(readListOfFiles(kMovingFilesMarker));
    boost::filesystem::remove(movingFilesMarker);
    fassertFailedNoTrace(5783416);
}

void InitialSyncFileMover::moveFilesAndHandleFailure(const std::vector<std::string>& filesToMove) {
    try {
        _moveFiles(filesToMove);
    } catch (boost::filesystem::filesystem_error& error) {
        LOGV2_FATAL_CONTINUE(
            5783415,
            "Failed while moving files to their final destination in initial sync. This is "
            "unrecoverable; the node must be resynced from the beginning",
            "error"_attr = error.what(),
            "path1"_attr = error.path1().string(),
            "path2"_attr = error.path2().string());

        _cleanupAfterFailedMoveAndFassert();
    }
}

void InitialSyncFileMover::deleteInitialSyncDir(std::string dbpath) {
    boost::filesystem::path initialSyncDir(dbpath);
    initialSyncDir.append(kInitialSyncDir.toString());
    boost::filesystem::remove_all(initialSyncDir);
}

}  // namespace repl
}  // namespace mongo
