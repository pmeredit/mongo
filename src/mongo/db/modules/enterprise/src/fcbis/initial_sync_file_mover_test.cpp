/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/filesystem.hpp>
#include <fstream>
#include <vector>

#include "initial_sync_file_mover.h"

#include "mongo/unittest/death_test.h"
#include "mongo/unittest/log_test.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/fail_point.h"

namespace mongo {
namespace repl {
class InitialSyncFileMoverTest : public unittest::Test {
    // In these scenarios, there are three old files, OLD1, OLD2, OLD3, and 3 new files NEW1, NEW2,
    // NEW3.  Each contains its own name in lower case.  There is also a common file COMMON, which
    // contains "old" for the one notionally existing and "new" for the one from the initial sync
    // source.  There is also one old directory OLDDIR, one new directory NEWDIR, and one common
    // directory COMMONDIR.  OLDDIR contains files OD1 and OD2, NEWDIR contains files ND1 and ND2,
    // each containing their own name in lowercase.  The COMMONDIR has files CDOLD1, CDOLD2 on the
    // old, CDNEW1, CDNEW2 on the new, and CDCOMMON on both. CDCOMMON contains "old" or "new", the
    // others contain their own name in lowercase.

protected:
    InitialSyncFileMoverTest()
        : _dbpathdir("InitialSyncFileMoverTest"),
          fileMover(_dbpathdir.path()),
          _dbpath(boost::filesystem::path(_dbpathdir.path())) {
        _initialSyncPath = _dbpath;
        _initialSyncPath.append(InitialSyncFileMover::kInitialSyncDir.toString());
    }

    bool markerExists(StringData markerName) {
        boost::filesystem::path markerPath(_dbpath);
        markerPath.append(markerName.toString());
        return boost::filesystem::exists(markerPath);
    }

    void assertNoMarkers() {
        ASSERT_FALSE(markerExists(InitialSyncFileMover::kMovingFilesMarker));
        ASSERT_FALSE(markerExists(InitialSyncFileMover::kMovingFilesTmpMarker));
        ASSERT_FALSE(markerExists(InitialSyncFileMover::kFilesToDeleteMarker));
        ASSERT_FALSE(markerExists(InitialSyncFileMover::kFilesToDeleteTmpMarker));
    }

    // Check if a file (relative to _dbpath) exists with the given contents.
    void assertExistsWithContents(StringData filepath, StringData expected_contents) {
        auto path = _dbpath;
        path.append(filepath.toString());
        ASSERT_TRUE(boost::filesystem::exists(path))
            << "File " << filepath << " should exist but does not";
        std::ifstream reader(path.native());
        std::string contents;
        reader >> contents;
        ASSERT_EQ(contents, expected_contents)
            << "File " << filepath << " should have contents " << expected_contents
            << " but instead has " << contents;
    }

    // Asserts that the system is in the condition expected when an initial sync node restarted
    // before an initial sync was complete.
    void assertInitialSyncFailedCleanly() {
        ASSERT_FALSE(boost::filesystem::exists(_initialSyncPath));
        assertNoMarkers();
        assertExistsWithContents("OLD1", "old1");
        assertExistsWithContents("OLD2", "old2");
        assertExistsWithContents("OLD3", "old3");
        assertExistsWithContents("OLDDIR/OD1", "od1");
        assertExistsWithContents("OLDDIR/OD2", "od2");
        assertExistsWithContents("COMMONDIR/CDOLD1", "cdold1");
        assertExistsWithContents("COMMONDIR/CDOLD2", "cdold2");
        assertExistsWithContents("COMMONDIR/CDCOMMON", "old");
        assertDoesNotExist("NEW1");
        assertDoesNotExist("NEW2");
        assertDoesNotExist("NEW3");
        assertDoesNotExist("NEWDIR");
        assertDoesNotExist("COMMONDIR/CDNEW1");
        assertDoesNotExist("COMMONDIR/CDNEW2");
    }

    // Ensure a file (relative to _dbpath) does not exist,
    virtual void assertDoesNotExist(StringData filepath) {
        auto path = _dbpath;
        path.append(filepath.toString());
        ASSERT_FALSE(boost::filesystem::exists(path))
            << "File " << filepath << " should not exist but does";
    }

    // Asserts that the system is in the condition expected when an initial sync node restarted
    // after an initial sync was complete but before the files have been moved.
    void assertInitialSyncCompleted() {
        ASSERT_FALSE(boost::filesystem::exists(_initialSyncPath));
        assertNoMarkers();
        assertExistsWithContents("NEW1", "new1");
        assertExistsWithContents("NEW2", "new2");
        assertExistsWithContents("NEW3", "new3");
        assertExistsWithContents("NEWDIR/ND1", "nd1");
        assertExistsWithContents("NEWDIR/ND2", "nd2");
        assertExistsWithContents("COMMONDIR/CDNEW1", "cdnew1");
        assertExistsWithContents("COMMONDIR/CDNEW2", "cdnew2");
        assertExistsWithContents("COMMONDIR/CDCOMMON", "new");
        assertDoesNotExist("OLD1");
        assertDoesNotExist("OLD2");
        assertDoesNotExist("OLD3");
        assertDoesNotExist("OLDDIR");
        assertDoesNotExist("COMMONDIR/CDOLD1");
        assertDoesNotExist("COMMONDIR/CDOLD2");
    }

    void writeFile(boost::filesystem::path path,
                   const std::string& name,
                   const std::string& contents) {
        boost::filesystem::path filepath(path);
        filepath.append(name);
        std::ofstream writer(filepath.native());
        writer << contents;
    }

    void writeMarker(StringData markerName,
                     std::vector<std::string> filenames,
                     bool corrupt = false) {
        boost::filesystem::path filepath(_dbpath);
        filepath.append(markerName.toString());
        std::ofstream writer(filepath.native());
        for (const auto& filename : filenames) {
            writer.write(filename.c_str(), filename.size() + 1);
        }
        writer.close();
        if (corrupt) {
            auto file_size = boost::filesystem::file_size(filepath);
            file_size /= 2;
            boost::filesystem::resize_file(filepath, file_size);
        }
    }

    void createOldFiles() {
        char filename[10];
        char contents[10];
        // OLD files.
        writeFile(_dbpath, "COMMON", "old");
        for (unsigned char i = 1; i <= 3; i++) {
            sprintf(filename, "OLD%hhu", i);
            sprintf(contents, "old%hhd", i);
            writeFile(_dbpath, filename, contents);
        }
        auto olddir = _dbpath;
        olddir.append("OLDDIR");
        ASSERT_TRUE(boost::filesystem::create_directory(olddir));
        for (unsigned char i = 1; i <= 2; i++) {
            sprintf(filename, "OD%hhu", i);
            sprintf(contents, "od%hhd", i);
            writeFile(olddir, filename, contents);
        }
        auto oldcommondir = _dbpath;
        oldcommondir.append("COMMONDIR");
        ASSERT_TRUE(boost::filesystem::create_directory(oldcommondir));
        for (unsigned char i = 1; i <= 2; i++) {
            sprintf(filename, "CDOLD%hhu", i);
            sprintf(contents, "cdold%hhu", i);
            writeFile(oldcommondir, filename, contents);
        }
        writeFile(oldcommondir, "CDCOMMON", "old");
    }

    void createNewFiles(const boost::filesystem::path& initialSyncPath) {
        char filename[10];
        char contents[10];
        for (unsigned char i = 1; i <= 3; i++) {
            sprintf(filename, "NEW%hhu", i);
            sprintf(contents, "new%hhu", i);
            writeFile(initialSyncPath, filename, contents);
        }
        auto newdir = initialSyncPath;
        newdir.append("NEWDIR");
        ASSERT_TRUE(boost::filesystem::create_directory(newdir));
        for (unsigned char i = 1; i <= 2; i++) {
            sprintf(filename, "ND%hhu", i);
            sprintf(contents, "nd%hhu", i);
            writeFile(newdir, filename, contents);
        }
        auto newcommondir = initialSyncPath;
        newcommondir.append("COMMONDIR");
        ASSERT_TRUE(boost::filesystem::create_directory(newcommondir));
        for (unsigned char i = 1; i <= 2; i++) {
            sprintf(filename, "CDNEW%hhu", i);
            sprintf(contents, "cdnew%hhu", i);
            writeFile(newcommondir, filename, contents);
        }
        writeFile(newcommondir, "CDCOMMON", "new");

        writeFile(initialSyncPath, "COMMON", "new");
    }

    void createNewFiles() {
        ASSERT_TRUE(boost::filesystem::create_directory(_initialSyncPath));
        createNewFiles(_initialSyncPath);
    }

    void createAllFiles() {
        createOldFiles();
        createNewFiles();
    }

    std::vector<std::string> moveMarkerContents() {
        return {"NEW1", "COMMON", "NEW2", "NEW3", "NEWDIR", "COMMONDIR"};
    }

    std::vector<std::string> deleteMarkerContents() {
        return {"OLD1",
                "COMMON",
                "OLD2",
                "OLD3",
                "OLDDIR/OD1",
                "OLDDIR/OD2",
                "COMMONDIR/CDOLD1",
                "COMMONDIR/CDCOMMON",
                "COMMONDIR/CDOLD2"};
    }

protected:
    unittest::TempDir _dbpathdir;
    InitialSyncFileMover fileMover;
    boost::filesystem::path _dbpath;
    boost::filesystem::path _initialSyncPath;

private:
    unittest::MinimumLoggedSeverityGuard replLogSeverityGuard{logv2::LogComponent::kReplication,
                                                              logv2::LogSeverity::Debug(3)};
};

// Fixture for test which checks we can handle symlinked directories to outside the dbpath.
class InitialSyncFileMoverSymlinkTest : public InitialSyncFileMoverTest {
protected:
    InitialSyncFileMoverSymlinkTest()
        : InitialSyncFileMoverTest(),
          _symlinkDestDir("InitialSyncFileMoverSymlinkTest"),
          _symlinkDestPath(boost::filesystem::path(_symlinkDestDir.path())) {}

    // Moves a directory (relative to dbpath) to the _symlinkDestDir and makes it a symlink
    void moveDirToSymlink(StringData sourceDir) {
        auto sourcePath = _dbpath;
        sourcePath.append(sourceDir.toString());
        for (auto& entry : boost::filesystem::directory_iterator(sourcePath)) {
            // We copy the files instead of rename/moving in case a subclass or test has
            // arranged for the symlink to point to another device.
            auto newPath = _symlinkDestPath;
            newPath /= entry.path().filename();
            boost::filesystem::copy(
                entry.path(), newPath, boost::filesystem::copy_options::recursive);
            boost::filesystem::remove_all(entry.path());
        }
        boost::filesystem::remove_all(sourcePath);
        boost::filesystem::create_directory_symlink(_symlinkDestPath, sourcePath);
    }

    // For the journal files, we create two files of each type -- WiredTigerLog,
    // WiredTigerTempLog, and WiredTigerPreplog -- in the old directory, and two in the new, with
    // one overlapping.
    //
    // When we're not moving the entire journal directory, we expect the regular log files to be
    // moved or copied, and the Temp and Prep files not to be moved or copied.  The duplicate log
    // file should be overwritten with no replacement.  All the pre-existing regular log files
    // should be removed even though they do not appear in the delete marker.
    void createAndSymlinkJournalFiles() {
        char filename[40];
        char contents[40];
        std::vector<std::string> logPrefixes = {
            InitialSyncFileMover::kWiredTigerLogPrefix.toString(),
            InitialSyncFileMover::kWiredTigerTmplogPrefix.toString(),
            InitialSyncFileMover::kWiredTigerTmplogPrefix.toString()};
        auto newJournalDir = _initialSyncPath;
        newJournalDir.append("journal");
        ASSERT_TRUE(boost::filesystem::create_directory(newJournalDir));
        for (unsigned char i = 1; i <= 2; i++) {
            for (auto& logPrefix : logPrefixes) {
                sprintf(filename, "%s.%010hhu", logPrefix.c_str(), i);
                sprintf(contents, "nj_%s.%010hhu", logPrefix.c_str(), i);
                writeFile(newJournalDir, filename, contents);
            }
        }

        auto oldJournalDir = _symlinkDestPath;
        for (unsigned char i = 2; i <= 3; i++) {
            for (auto& logPrefix : logPrefixes) {
                sprintf(filename, "%s.%010hhu", logPrefix.c_str(), i);
                sprintf(contents, "oj %s.%010hhu", logPrefix.c_str(), i);
                writeFile(oldJournalDir, filename, contents);
            }
        }
        auto sourcePath = _dbpath;
        sourcePath.append("journal");
        boost::filesystem::create_directory_symlink(_symlinkDestPath, sourcePath);
    }

    // The new journal directory should have the pre-existing temp and preplog files,
    // and the new log files, and nothing else.
    void checkJournalFiles() {
        assertIsSymlink("journal");
        auto journalPath = _dbpath;
        journalPath.append("journal");
        StringSet expectedFiles = {InitialSyncFileMover::kWiredTigerPreplogPrefix + ".0000000002",
                                   InitialSyncFileMover::kWiredTigerPreplogPrefix + ".0000000003",
                                   InitialSyncFileMover::kWiredTigerTmplogPrefix + ".0000000002",
                                   InitialSyncFileMover::kWiredTigerTmplogPrefix + ".0000000003",
                                   InitialSyncFileMover::kWiredTigerLogPrefix + ".0000000001",
                                   InitialSyncFileMover::kWiredTigerLogPrefix + ".0000000002"};
        StringSet actualFiles;
        for (const auto& dirEntry : boost::filesystem::directory_iterator(journalPath)) {
            std::string fileName = dirEntry.path().filename().generic_string();
            ASSERT(expectedFiles.find(fileName) != expectedFiles.end())
                << "File " << fileName
                << " was found in the journal directory but should not be there";
            actualFiles.insert(fileName);
        }
        for (auto& fileName : expectedFiles) {
            ASSERT(expectedFiles.find(fileName) != expectedFiles.end())
                << "File " << fileName
                << " was expected to be found in the journal directory but was not there.";
        }

        // Make sure the log files which exist have the proper contents.  WiredTiger is just going
        // to delete the prep and tmp files anyway, so no need to check their contents.
        using namespace std::string_literals;
        assertExistsWithContents(
            "journal/"s + InitialSyncFileMover::kWiredTigerLogPrefix + ".0000000001",
            "nj_"s + InitialSyncFileMover::kWiredTigerLogPrefix + ".0000000001");
        assertExistsWithContents(
            "journal/"s + InitialSyncFileMover::kWiredTigerLogPrefix + ".0000000002",
            "nj_"s + InitialSyncFileMover::kWiredTigerLogPrefix + ".0000000002");
    }

    // For this test, we allow symlinks to empty directories count as "does not exist".
    void assertDoesNotExist(StringData filepath) override {
        auto path = _dbpath;
        path.append(filepath.toString());
        if (boost::filesystem::is_symlink(path)) {
            auto symlinkDest = boost::filesystem::read_symlink(path);
            if (boost::filesystem::is_directory(path) && boost::filesystem::is_empty(path))
                return;
        }
        ASSERT_FALSE(boost::filesystem::exists(path))
            << "File " << filepath << " should not exist but does";
    }

    void assertIsSymlink(StringData filepath) {
        auto path = _dbpath;
        path.append(filepath.toString());
        auto symlinkDest = boost::filesystem::read_symlink(path);
        ASSERT_TRUE(symlinkDest == _symlinkDestPath)
            << "File " << filepath << " should be a symlink to our symlink dir but is not.";
    }

protected:
    unittest::TempDir _symlinkDestDir;
    boost::filesystem::path _symlinkDestPath;
};

// The normal case when there are no initial sync files.
TEST_F(InitialSyncFileMoverTest, Recovery_NoInitialSyncDir) {
    createOldFiles();
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncFailedCleanly();
}

// The scenario where we crashed before writing the list of files to delete.  We don't try to
// recover from this point.
TEST_F(InitialSyncFileMoverTest, Recovery_NoMarkerExists) {
    createAllFiles();
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncFailedCleanly();
}

// The scenario where we crashed when writing the list of files to delete.  We don't try to
// recover from this point.
TEST_F(InitialSyncFileMoverTest, Recovery_TempDeleteMarkerExists) {
    createAllFiles();
    writeMarker(
        InitialSyncFileMover::kFilesToDeleteTmpMarker, deleteMarkerContents(), true /*corrupt*/);
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncFailedCleanly();
}

// The scenario where we wrote the list of files to delete but did nothing more.  We can complete
// initial sync.
TEST_F(InitialSyncFileMoverTest, Recovery_DeleteMarkerExists) {
    createAllFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

// The scenario where we wrote the list of files to delete and deleted some files.  We can complete
// initial sync.
TEST_F(InitialSyncFileMoverTest, Recovery_DeleteStarted) {
    createAllFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    auto path = _dbpath;
    path.append("OLD1");
    boost::filesystem::remove(path);
    path = _dbpath;
    path.append("OLDDIR/OD11");
    boost::filesystem::remove(path);
    path = _dbpath;
    path.append("COMMONDIR/CDOLD1");
    boost::filesystem::remove(path);
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

// The scenario where we deleted all the files and wrote a temporary move marker.  We can complete
// the initial sync.
TEST_F(InitialSyncFileMoverTest, Recovery_TempMoveMarkerExists) {
    createNewFiles();
    // Since we should not remove the delete marker until the move marker is written, we should
    // have a delete marker at this time.
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    writeMarker(
        InitialSyncFileMover::kMovingFilesTmpMarker, moveMarkerContents(), /* corrupt = */ true);
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

// The scenario where we deleted all the files and wrote a move marker.  We can complete the initial
// sync.
TEST_F(InitialSyncFileMoverTest, Recovery_MoveAndDeleteMarkersExists) {
    createNewFiles();
    // Since we should not remove the delete marker until the move marker is written, we could
    // have a delete marker at this time.
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    writeMarker(InitialSyncFileMover::kMovingFilesMarker, moveMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

// Same as above, only we also got as far as removing the delete marker.
TEST_F(InitialSyncFileMoverTest, Recovery_MoveMarkerExists) {
    createNewFiles();
    writeMarker(InitialSyncFileMover::kMovingFilesMarker, moveMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

// The scenario where we moved some of the files.  We can complete the initial sync.
TEST_F(InitialSyncFileMoverTest, Recovery_SomeFilesMoved) {
    createNewFiles();
    boost::filesystem::path oldpath(_initialSyncPath);
    boost::filesystem::path newpath(_dbpath);
    oldpath.append("NEW1");
    newpath.append("NEW1");
    // This assert is to verify the test is actually doing something.
    ASSERT_TRUE(boost::filesystem::exists(oldpath));
    boost::filesystem::rename(oldpath, newpath);
    oldpath = _initialSyncPath;
    newpath = _dbpath;
    oldpath.append("NEWDIR");
    newpath.append("NEWDIR");
    ASSERT_TRUE(boost::filesystem::exists(oldpath));
    boost::filesystem::rename(oldpath, newpath);
    writeMarker(InitialSyncFileMover::kMovingFilesMarker, moveMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

// The scenario where we moved all of the files but still have a move marker.
TEST_F(InitialSyncFileMoverTest, Recovery_AllFilesMoved) {
    ASSERT_TRUE(boost::filesystem::create_directory(_initialSyncPath));
    createNewFiles(_dbpath);
    writeMarker(InitialSyncFileMover::kMovingFilesMarker, moveMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

// Testing that when there is a file that is not part of the files to delete and it's empty, that
// we succeed anyway.
TEST_F(InitialSyncFileMoverTest, Recovery_ConflictingEmptyFile) {
    createAllFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    auto conflictPath = _dbpath;
    conflictPath.append("NEW1");
    std::ofstream writer(conflictPath.native());
    writer.close();
    ASSERT_TRUE(boost::filesystem::exists(conflictPath));
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    for (auto dirIter = boost::filesystem::directory_iterator(_dbpath);
         dirIter != boost::filesystem::directory_iterator();
         dirIter++) {
        std::string filename(dirIter->path().filename().string());
        ASSERT_TRUE(filename == "NEW1" || !StringData(filename).startsWith("NEW1"))
            << "Expected file " << filename << " not to exist.";
    }
}

// Testing that when there is a file that is not part of the files to delete and it's not empty,
// that we succeed anyway and move the file somewhere else.
TEST_F(InitialSyncFileMoverTest, Recovery_ConflictingNonEmptyFile) {
    createAllFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    auto conflictPath = _dbpath;
    conflictPath.append("NEW1");
    std::ofstream writer(conflictPath.native());
    writer << "conflict";
    writer.close();
    ASSERT_TRUE(boost::filesystem::exists(conflictPath));
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    std::string foundFile;
    for (auto dirIter = boost::filesystem::directory_iterator(_dbpath);
         foundFile.empty() && dirIter != boost::filesystem::directory_iterator();
         dirIter++) {
        std::string filename(dirIter->path().filename().string());
        if (filename != "NEW1" && StringData(filename).startsWith("NEW1")) {
            foundFile = filename;
        }
    }
    ASSERT_FALSE(foundFile.empty()) << "Conflict file was deleted instead of moved";
}
// Testing that when there is a directory that is not part of the files to delete and it's empty,
// that we succeed anyway.
TEST_F(InitialSyncFileMoverTest, Recovery_ConflictingEmptyDirectory) {
    createAllFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    auto conflictPath = _dbpath;
    conflictPath.append("NEWDIR");
    boost::filesystem::create_directory(conflictPath);
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    for (auto dirIter = boost::filesystem::directory_iterator(_dbpath);
         dirIter != boost::filesystem::directory_iterator();
         dirIter++) {
        std::string filename(dirIter->path().filename().string());
        ASSERT_TRUE(filename == "NEWDIR" || !StringData(filename).startsWith("NEWDIR"))
            << "Expected file " << filename << " not to exist.";
    }
}

// Testing that when there is a file that is not part of the files to delete and it's not empty,
// that we succeed anyway and move the file somewhere else.
TEST_F(InitialSyncFileMoverTest, Recovery_ConflictingNonEmptyDirectory) {
    createAllFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    auto conflictPath = _dbpath;
    conflictPath.append("NEWDIR");
    auto conflictSubPath = conflictPath;
    conflictSubPath.append("CONFLICTPATH");
    boost::filesystem::create_directory(conflictPath);
    boost::filesystem::create_directory(conflictSubPath);
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    std::string foundFile;
    for (auto dirIter = boost::filesystem::directory_iterator(_dbpath);
         foundFile.empty() && dirIter != boost::filesystem::directory_iterator();
         dirIter++) {
        std::string filename(dirIter->path().filename().string());
        if (filename != "NEWDIR" && StringData(filename).startsWith("NEWDIR")) {
            foundFile = filename;
        }
    }
    ASSERT_FALSE(foundFile.empty()) << "Conflict directory was deleted instead of moved";
}

TEST_F(InitialSyncFileMoverTest, RemoveInitialSyncDirectoryTestDoesNotExist) {
    InitialSyncFileMover::deleteInitialSyncDir(_dbpath.string());
    ASSERT_FALSE(boost::filesystem::exists(_initialSyncPath));
    ASSERT_TRUE(boost::filesystem::is_empty(_dbpath));
}

TEST_F(InitialSyncFileMoverTest, RemoveNonEmptyInitialSyncDirectoryTest) {
    ASSERT_TRUE(boost::filesystem::create_directory(_initialSyncPath));
    createNewFiles(_initialSyncPath);
    InitialSyncFileMover::deleteInitialSyncDir(_dbpath.string());
    ASSERT_FALSE(boost::filesystem::exists(_initialSyncPath));
    ASSERT_TRUE(boost::filesystem::is_empty(_dbpath));
}

TEST_F(InitialSyncFileMoverSymlinkTest, OldDirIsSymlink) {
    createAllFiles();
    moveDirToSymlink("OLDDIR");
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    assertIsSymlink("OLDDIR");
}

TEST_F(InitialSyncFileMoverSymlinkTest, CommonDirIsSymlink) {
    createAllFiles();
    moveDirToSymlink("COMMONDIR");
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    assertIsSymlink("COMMONDIR");
}

TEST_F(InitialSyncFileMoverSymlinkTest, CommonDirIsSymlinkWithCopy) {
    FailPointEnableBlock fp("initialSyncFileMoverAlwaysCopy");
    createAllFiles();
    moveDirToSymlink("COMMONDIR");
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    assertIsSymlink("COMMONDIR");
}

TEST_F(InitialSyncFileMoverSymlinkTest, JournalDirIsSymlink) {
    createAllFiles();
    createAndSymlinkJournalFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    checkJournalFiles();
}

TEST_F(InitialSyncFileMoverSymlinkTest, JournalDirIsSymlinkWithCopy) {
    FailPointEnableBlock fp("initialSyncFileMoverAlwaysCopy");
    createAllFiles();
    createAndSymlinkJournalFiles();
    writeMarker(InitialSyncFileMover::kFilesToDeleteMarker, deleteMarkerContents());
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
    checkJournalFiles();
}

// The scenario where a file in the move marker is missing.
DEATH_TEST_F(InitialSyncFileMoverTest, Recovery_MovedFileMissing, "5783416") {
    ASSERT_TRUE(boost::filesystem::create_directory(_initialSyncPath));
    createNewFiles(_dbpath);
    writeMarker(InitialSyncFileMover::kMovingFilesMarker, moveMarkerContents());
    auto missingPath = _dbpath;
    missingPath.append("NEW1");
    boost::filesystem::remove(missingPath);
    fileMover.recoverFileCopyBasedInitialSyncAtStartup();
    assertInitialSyncCompleted();
}

}  // namespace repl
}  // namespace mongo
