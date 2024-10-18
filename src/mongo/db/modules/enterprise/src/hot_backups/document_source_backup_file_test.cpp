/**
 * Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include <boost/filesystem.hpp>
#include <cstring>
#include <fstream>

#include "backup_cursor_service.h"
#include "document_source_backup_cursor.h"
#include "document_source_backup_file.h"
#include "mock_mongo_interface_for_backup_tests.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/process_interface/stub_mongo_process_interface.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/devnull/devnull_kv_engine.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

namespace mongo {
namespace {

// TODO SERVER-83935 move these helpers into some pipeline testing library.
auto serializeToBson(const auto& stage, const SerializationOptions& opts = {}) {
    std::vector<Value> array;
    stage->serializeToArray(array, opts);
    return array[0].getDocument().toBson();
}

auto debugShape(const auto& stage) {
    return serializeToBson(stage, SerializationOptions::kDebugQueryShapeSerializeOptions);
}

auto representativeQueryShape(const auto& stage) {
    return serializeToBson(stage, SerializationOptions::kRepresentativeQueryShapeSerializeOptions);
}

class DocumentSourceBackupFileTest : public ServiceContextMongoDTest {
public:
    DocumentSourceBackupFileTest()
        : ServiceContextMongoDTest(Options{}.engine("devnull")),
          _opCtx(cc().makeOperationContext()) {
        repl::ReplicationCoordinator::set(
            getServiceContext(),
            std::make_unique<repl::ReplicationCoordinatorMock>(getServiceContext()));
        repl::StorageInterface::set(getServiceContext(),
                                    std::make_unique<repl::StorageInterfaceMock>());
        BackupCursorHooks::initialize(getServiceContext());
    }

    // A helper function to create a backup cursor and get the backupId to pass into the test
    // objects below.
    UUID createBackupCursorStage(const boost::intrusive_ptr<ExpressionContext>& expCtx) {
        auto devNullEngine = static_cast<DevNullKVEngine*>(
            _opCtx->getClient()->getServiceContext()->getStorageEngine()->getEngine());
        devNullEngine->setBackupBlocks_forTest({BackupBlock(/*nss=*/boost::none,
                                                            /*uuid=*/boost::none,
                                                            fileToBackup)});

        // Set up the $backupCursor stage.
        auto backupCursorSpec = fromjson("{$backupCursor: {}}");

        invariant(_backupCursorStage == nullptr);
        _backupCursorStage =
            DocumentSourceBackupCursor::createFromBson(backupCursorSpec.firstElement(), expCtx);

        // Call `getNext()` once, to retrieve the metadata document.
        _backupCursorStage->getNext();
        // Get 'testFile.txt'.
        _backupCursorStage->getNext();

        auto svcCtx = _opCtx->getClient()->getServiceContext();
        auto backupCursorService =
            static_cast<BackupCursorService*>(BackupCursorService::get(svcCtx));
        return backupCursorService->getBackupId();
    }


    boost::intrusive_ptr<DocumentSourceBackupFile> testCreateFromBsonResult(
        const BSONObj backupFileSpec, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
        return DocumentSourceBackupFile::createFromBson(backupFileSpec.firstElement(), expCtx);
    }

    // A helper function write 'fileContent' to the file path 'fileToBackup'. This is used for
    // testing file copying.
    void createMockFileToCopy(const std::string fileContent) {
        std::fstream f;
        f.open(fileToBackup, std::ios_base::out);
        ASSERT_TRUE(f.is_open());
        f << fileContent;
        f.close();
        ASSERT(boost::filesystem::exists(fileToBackup));
    }

    void closeBackupCursor() {
        // Simulates a timeout of the _backupCursor.
        _backupCursorStage = nullptr;
    }

protected:
    ServiceContext::UniqueOperationContext _opCtx;
    // This file path will be returned by the backup cursor to the backupFile stage.
    std::string fileToBackup = storageGlobalParams.dbpath + "/testFile.txt";
    // The _backupCursorStage must be kept around while the backup file is being retrieved.
    boost::intrusive_ptr<DocumentSource> _backupCursorStage;
};

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingValidSpec) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    ASSERT_DOES_NOT_THROW(testCreateFromBsonResult(spec, expCtx));
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingValidSpecWithByteOffsetAndLength) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec =
        BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup << "byteOffset"
                                              << 1LL << "length" << 1LL));
    ASSERT_DOES_NOT_THROW(testCreateFromBsonResult(spec, expCtx));
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecNotObject) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    BSONObj spec = BSON("$backupFile" << 1);
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::FailedToParse);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecMissingBackupId) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    BSONObj spec = BSON("$backupFile"
                        << BSON("file" << fileToBackup << "byteOffset" << 1LL << "length" << 1LL));
    ASSERT_THROWS_CODE(testCreateFromBsonResult(spec, expCtx), DBException, 40414);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidBackupId) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId"
                                              << "12345"
                                              << "file" << fileToBackup));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecMissingFilePath) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId));
    ASSERT_THROWS_CODE(testCreateFromBsonResult(spec, expCtx), DBException, 40414);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidFilePath) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << 12345));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidByteOffsetType) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup
                                                         << "byteOffset" << 1));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidLengthType) {
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile"
                        << BSON("backupId" << backupId << "file" << fileToBackup << "length" << 1));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

// Tests that the serialization behaves as we expect. Mostly this is important for testing the query
// shape of $backupFile.
TEST_F(DocumentSourceBackupFileTest, TestSerialization) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);
    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    BSONObj representativeShape;
    {
        auto stage = DocumentSourceBackupFile::createFromBson(spec.firstElement(), expCtx);
        ASSERT_BSONOBJ_EQ(
            BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup
                                                  << "byteOffset" << 0 << "length" << -1)),
            serializeToBson(stage));
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
                "$backupFile": {
                    "backupId": "?binData",
                    "file": "?string",
                    "byteOffset": "?number",
                    "length": "?number"
                }
            })",
            debugShape(stage));

        representativeShape = representativeQueryShape(stage);
        ASSERT_BSONOBJ_EQ_AUTO(  // NOLINT
            R"({
                "$backupFile": {
                    "backupId": {"$uuid":"00000000-0000-4000-8000-000000000000"},
                    "file": "?",
                    "byteOffset": 1,
                    "length": 1
                }
            })",
            representativeShape);
    }

    // Test that we are able to parse the representative query.
    auto reParsed =
        DocumentSourceBackupFile::createFromBson(representativeShape.firstElement(), expCtx);

    // Test that the representative query is a fix point - should always get here.
    ASSERT_BSONOBJ_EQ(representativeShape, representativeQueryShape(reParsed));
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageValidCopyingFile) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    Document expectedObj = Document{BSON("byteOffset" << 0 << "endOfFile" << true << "data"
                                                      << BSONBinData(fileContent.data(),
                                                                     fileContent.length(),
                                                                     BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    ASSERT_TRUE(backupFile->getNext().isEOF());
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageValidCopyingFileWithOffset) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup
                                                         << "byteOffset" << 1LL));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    std::string expectedData = "123456789";
    Document expectedObj = Document{BSON("byteOffset" << 1 << "endOfFile" << true << "data"
                                                      << BSONBinData(expectedData.data(),
                                                                     expectedData.length(),
                                                                     BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    ASSERT_TRUE(backupFile->getNext().isEOF());
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageValidCopyingFileWithLength) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON(
        "$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup << "length" << 2LL));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    std::string expectedData = "01";
    Document expectedObj = Document{BSON("byteOffset" << 0 << "data"
                                                      << BSONBinData(expectedData.data(),
                                                                     expectedData.length(),
                                                                     BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    ASSERT_TRUE(backupFile->getNext().isEOF());
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageValidCopyingFileWithOffsetAndLength) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec =
        BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup << "byteOffset"
                                              << 2LL << "length" << 2LL));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    std::string expectedData = "23";
    Document expectedObj = Document{BSON("byteOffset" << 2 << "data"
                                                      << BSONBinData(expectedData.data(),
                                                                     expectedData.length(),
                                                                     BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    ASSERT_TRUE(backupFile->getNext().isEOF());
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageValidCopyingFileLargerThanMaxBsonSize) {
    auto fileContent = std::string(BSONObjMaxUserSize, '0');
    fileContent += "123456789";
    ASSERT_EQ(fileContent.size(), BSONObjMaxUserSize + 9);
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    ASSERT_EQ(next.getDocument().getField("data").getBinData().length, BSONObjMaxUserSize - 1);
    ASSERT_EQ(next.getDocument().getField("byteOffset").getLong(), 0);

    next = backupFile->getNext();
    std::string expectedData = "0123456789";
    Document expectedObj =
        Document{BSON("byteOffset" << BSONObjMaxUserSize - 1 << "endOfFile" << true << "data"
                                   << BSONBinData(expectedData.data(),
                                                  expectedData.length(),
                                                  BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    ASSERT_TRUE(backupFile->getNext().isEOF());
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageValidCopyingFileMultipleDocumentsUntilEOF) {
    auto fileContent = std::string(BSONObjMaxUserSize * 3, '0');
    fileContent += "123";
    ASSERT_EQ(fileContent.size(), (BSONObjMaxUserSize * 3) + 3);
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    ASSERT_EQ(next.getDocument().getField("data").getBinData().length, BSONObjMaxUserSize - 1);
    ASSERT_EQ(next.getDocument().getField("byteOffset").getLong(), 0);

    next = backupFile->getNext();
    ASSERT_EQ(next.getDocument().getField("data").getBinData().length, BSONObjMaxUserSize - 1);
    ASSERT_EQ(next.getDocument().getField("byteOffset").getLong(), BSONObjMaxUserSize - 1);

    next = backupFile->getNext();
    ASSERT_EQ(next.getDocument().getField("data").getBinData().length, BSONObjMaxUserSize - 1);
    ASSERT_EQ(next.getDocument().getField("byteOffset").getLong(), (BSONObjMaxUserSize - 1) * 2);

    next = backupFile->getNext();
    std::string expectedData = "000123";
    Document expectedObj =
        Document{BSON("byteOffset" << (BSONObjMaxUserSize * 3) - 3 << "endOfFile" << true << "data"
                                   << BSONBinData(expectedData.data(),
                                                  expectedData.length(),
                                                  BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    ASSERT_TRUE(backupFile->getNext().isEOF());
}

TEST_F(DocumentSourceBackupFileTest,
       TestBackupFileStageValidCopyingFileMultipleDocumentsUntilLength) {
    auto fileContent = std::string(BSONObjMaxUserSize, '0');
    fileContent = fileContent + "1234";
    ASSERT_EQ(fileContent.size(), BSONObjMaxUserSize + 4);
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec =
        BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup << "length"
                                              << static_cast<long long>(BSONObjMaxUserSize + 2)));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    ASSERT_EQ(next.getDocument().getField("data").getBinData().length, BSONObjMaxUserSize - 1);
    ASSERT_EQ(next.getDocument().getField("byteOffset").getLong(), 0);

    next = backupFile->getNext();
    std::string expectedData = "012";
    Document expectedObj = Document{BSON("byteOffset" << BSONObjMaxUserSize - 1 << "data"
                                                      << BSONBinData(expectedData.data(),
                                                                     expectedData.length(),
                                                                     BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    ASSERT_TRUE(backupFile->getNext().isEOF());
}


TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageInvalidCopyingWithZeroLength) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON(
        "$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup << "length" << 0LL));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    ASSERT_TRUE(backupFile->getNext().isEOF());
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageInvalidCopyingFileBackupClosed) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    closeBackupCursor();
    ASSERT_THROWS_CODE(backupFile->getNext(), DBException, ErrorCodes::IllegalOperation);
    // Subsequent calls should also return the error.
    ASSERT_THROWS_CODE(backupFile->getNext(), DBException, ErrorCodes::IllegalOperation);
}

TEST_F(DocumentSourceBackupFileTest, InvalidCopyingFileBackupClosedWhileIterating) {
    auto fileContent = std::string(BSONObjMaxUserSize * 3, '0');
    fileContent += "123";
    ASSERT_EQ(fileContent.size(), (BSONObjMaxUserSize * 3) + 3);
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    auto next = backupFile->getNext();
    ASSERT_EQ(next.getDocument().getField("data").getBinData().length, BSONObjMaxUserSize - 1);
    ASSERT_EQ(next.getDocument().getField("byteOffset").getLong(), 0);

    next = backupFile->getNext();
    ASSERT_EQ(next.getDocument().getField("data").getBinData().length, BSONObjMaxUserSize - 1);
    ASSERT_EQ(next.getDocument().getField("byteOffset").getLong(), BSONObjMaxUserSize - 1);

    closeBackupCursor();
    ASSERT_THROWS_CODE(backupFile->getNext(), DBException, 7124700);
    // Subsequent calls should also return the error.
    ASSERT_THROWS_CODE(backupFile->getNext(), DBException, 7124700);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageInvalidAtEOFBackupClosed) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createMockBackupExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);
    auto next = backupFile->getNext();
    Document expectedObj = Document{BSON("byteOffset" << 0 << "endOfFile" << true << "data"
                                                      << BSONBinData(fileContent.data(),
                                                                     fileContent.length(),
                                                                     BinDataType::BinDataGeneral))};
    ASSERT_DOCUMENT_EQ(expectedObj, next.getDocument());
    closeBackupCursor();
    // This is OK, because we already hit EOF and the backup cursor service will not attempt to
    // read again.
    ASSERT_TRUE(backupFile->getNext().isEOF());
}

}  // namespace
}  // namespace mongo
