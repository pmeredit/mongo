/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

#include "backup_cursor_service.h"
#include "document_source_backup_cursor.h"
#include "document_source_backup_file.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/document_value_test_util.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_mock.h"
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
        : ServiceContextMongoDTest("devnull"),
          _opCtx(cc().makeOperationContext()),
          _storageEngine(getServiceContext()->getStorageEngine()) {
        repl::ReplicationCoordinator::set(
            getServiceContext(),
            std::make_unique<repl::ReplicationCoordinatorMock>(getServiceContext()));
        repl::StorageInterface::set(getServiceContext(),
                                    std::make_unique<repl::StorageInterfaceMock>());
        BackupCursorHooks::initialize(getServiceContext(), _storageEngine);
    }

    // A helper function to create a backup cursor and get the backupId to pass into the test
    // objects below.
    UUID createBackupCursorStage(const boost::intrusive_ptr<ExpressionContext>& expCtx) {
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


    void testCreateFromBsonResult(const BSONObj backupFileSpec,
                                  const boost::intrusive_ptr<ExpressionContext>& expCtx) {
        DocumentSourceBackupFile::createFromBson(backupFileSpec.firstElement(), expCtx);
    }

protected:
    ServiceContext::UniqueOperationContext _opCtx;
    StorageEngine* _storageEngine;
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
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file"
                                                         << "filename.wt"));
    ASSERT_DOES_NOT_THROW(testCreateFromBsonResult(spec, expCtx));
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingValidSpecWithByteOffsetAndLength) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec =
        BSON("$backupFile" << BSON("backupId" << backupId << "file"
                                              << "filename.wt"
                                              << "byteOffset" << 1LL << "length" << 1LL));
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
    BSONObj spec = BSON("$backupFile" << BSON("file"
                                              << "filename.wt"
                                              << "byteOffset" << 1LL << "length" << 1LL));
    ASSERT_THROWS_CODE(testCreateFromBsonResult(spec, expCtx), DBException, 40414);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidBackupId) {
    auto expCtx = createExpressionContext(_opCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId"
                                              << "12345"
                                              << "file"
                                              << "filename.wt"));
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

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidByteOffsetType) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file"
                                                         << "filename.wt"
                                                         << "byteOffset" << 1));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}

TEST_F(DocumentSourceBackupFileTest, TestBackupFileStageParsingInvalidSpecInvalidLengthType) {
    auto expCtx = createExpressionContext(_opCtx);
    auto backupId = createBackupCursorStage(expCtx);
    BSONObj spec = BSON("$backupFile" << BSON("backupId" << backupId << "file"
                                                         << "filename.wt"
                                                         << "length" << 1));
    ASSERT_THROWS_CODE(
        testCreateFromBsonResult(spec, expCtx), DBException, ErrorCodes::TypeMismatch);
}
}  // namespace
}  // namespace mongo
