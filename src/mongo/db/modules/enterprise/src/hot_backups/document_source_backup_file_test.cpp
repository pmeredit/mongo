/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

#include <boost/filesystem.hpp>
#include <fstream>
#include <string.h>

#include "backup_cursor_service.h"
#include "document_source_backup_cursor.h"
#include "document_source_backup_file.h"
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
#include "mongo/db/storage/storage_engine_impl.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

#define ASSERT_DOES_NOT_THROW(EXPRESSION)                                          \
    try {                                                                          \
        EXPRESSION;                                                                \
    } catch (const AssertionException& e) {                                        \
        str::stream err;                                                           \
        err << "Threw an exception incorrectly: " << e.toString();                 \
        ::mongo::unittest::TestAssertionFailure(__FILE__, __LINE__, err).stream(); \
    }

namespace mongo {
namespace {
/**
 * A MongoProcessInterface used for testing that directs DocumentSourceBackupCursor to open a
 * backup cursor through BackupCursorService. We require a backup cursor to be open
 * so we can verify the 'backupId' field passed into the BackupFile stage matches the currently open
 * backup cursor.
 */
class MockMongoInterface final : public StubMongoProcessInterface {
public:
    BackupCursorState openBackupCursor(OperationContext* opCtx,
                                       const StorageEngine::BackupOptions& options) override {
        auto svcCtx = opCtx->getClient()->getServiceContext();
        auto backupCursorService =
            static_cast<BackupCursorService*>(BackupCursorService::get(svcCtx));
        return backupCursorService->openBackupCursor(opCtx, options);
    }
};

class DocumentSourceBackupFileTest : public ServiceContextMongoDTest {
public:
    DocumentSourceBackupFileTest()
        : ServiceContextMongoDTest("devnull"), _opCtx(cc().makeOperationContext()) {
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
        devNullEngine->setBackupBlocks_forTest({BackupBlock(fileToBackup)});

        // Set up the $backupCursor stage.
        auto backupCursorSpec = fromjson("{$backupCursor: {}}");

        auto backupCursorStage =
            DocumentSourceBackupCursor::createFromBson(backupCursorSpec.firstElement(), expCtx);

        // Call `getNext()` once, to retrieve the metadata document.
        backupCursorStage->getNext();
        // Get 'filename.wt'.
        backupCursorStage->getNext();

        auto svcCtx = _opCtx->getClient()->getServiceContext();
        auto backupCursorService =
            static_cast<BackupCursorService*>(BackupCursorService::get(svcCtx));
        return backupCursorService->getBackupId_forTest();
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

protected:
    ServiceContext::UniqueOperationContext _opCtx;
    // This file path will be returned by the backup cursor to the backupFile stage.
    std::string fileToBackup = storageGlobalParams.dbpath + "/filename.wt";
};

const boost::intrusive_ptr<ExpressionContext> createExpressionContext(
    const ServiceContext::UniqueOperationContext& opCtx) {
    auto expCtx = make_intrusive<ExpressionContext>(
        opCtx.get(), nullptr, NamespaceString::makeCollectionlessAggregateNSS("unittest"));
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>();
    return expCtx;
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingValidSpec) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup));
    ASSERT_DOES_NOT_THROW(testCreateFromBsonResult(spec, expCtx));
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingValidSpecWithByteOffsetAndLength) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec =
        BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup << "byteOffset"
                                              << 1LL << "length" << 1LL));
    ASSERT_DOES_NOT_THROW(testCreateFromBsonResult(spec, expCtx));
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecNotObject) {
    auto expCtx = createExpressionContext(_opCtx);
    BSONObj spec = BSON("$backupFile" << 1);
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::FailedToParse);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecMissingBackupId) {
    auto expCtx = createExpressionContext(_opCtx);
    BSONObj spec = BSON("$backupFile"
                        << BSON("file" << fileToBackup << "byteOffset" << 1LL << "length" << 1LL));
    ASSERT_THROWS_CODE(testCreateFromBsonResult(spec, expCtx), DBException, 40414);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidBackupId) {
    auto expCtx = createExpressionContext(_opCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId"
                                              << "12345"
                                              << "file" << fileToBackup));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecMissingFilePath) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId));
    ASSERT_THROWS_CODE(testCreateFromBsonResult(spec, expCtx), DBException, 40414);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidFilePath) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << 12345));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest,
       TestBackupFileStageParsingInvalidSpecFilePathNotReturnedByActiveBackupCursor) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file"
                                                         << "invalidFileName.wt"));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::IllegalOperation);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidByteOffsetType) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup
                                                         << "byteOffset" << 1));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidLengthType) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile"
                        << BSON("backupId" << backupId << "file" << fileToBackup << "length" << 1));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageValidCopyingFile) {
    std::string fileContent = "0123456789";
    createMockFileToCopy(fileContent);

    auto expCtx = createExpressionContext(_opCtx);
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

    auto expCtx = createExpressionContext(_opCtx);
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

    auto expCtx = createExpressionContext(_opCtx);
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

    auto expCtx = createExpressionContext(_opCtx);
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

    auto expCtx = createExpressionContext(_opCtx);
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

    auto expCtx = createExpressionContext(_opCtx);
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

    auto expCtx = createExpressionContext(_opCtx);
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

    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON(
        "$backupFile" << BSON("backupId" << backupId << "file" << fileToBackup << "length" << 0LL));
    auto backupFile = testCreateFromBsonResult(spec, expCtx);

    ASSERT_TRUE(backupFile->getNext().isEOF());
}


}  // namespace
}  // namespace mongo
