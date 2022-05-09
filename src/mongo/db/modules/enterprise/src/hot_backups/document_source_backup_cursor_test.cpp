/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */


#include "mongo/db/pipeline/variables.h"
#include "mongo/platform/basic.h"

#include "document_source_backup_cursor.h"

#include "backup_cursor_service.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/process_interface/stub_mongo_process_interface.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/devnull/devnull_kv_engine.h"
#include "mongo/db/storage/storage_engine_impl.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/unittest.h"
#include <memory>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery


namespace mongo {
namespace {

/**
 * A MongoProcessInterface used for testing that directs DocumentSourceBackupCursor to open a
 * backup cursor through BackupCursorService.
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

class DocumentSourceBackupCursorTest : public ServiceContextMongoDTest {
public:
    DocumentSourceBackupCursorTest()
        : ServiceContextMongoDTest(Options{}.engine("devnull")),
          _opCtx(cc().makeOperationContext()) {
        repl::ReplicationCoordinator::set(
            getServiceContext(),
            std::make_unique<repl::ReplicationCoordinatorMock>(getServiceContext()));
        repl::StorageInterface::set(getServiceContext(),
                                    std::make_unique<repl::StorageInterfaceMock>());
        BackupCursorHooks::initialize(getServiceContext());
    }

protected:
    ServiceContext::UniqueOperationContext _opCtx;
};

// Tests that the files retrieved in $backupCursor through DocumentSourceBackupCursor
// are correctly tracked and checked in BackupCursorService.
TEST_F(DocumentSourceBackupCursorTest, TestFilenameCheck) {
    // Set up MockMongoInterface.
    auto expCtx = new ExpressionContext(
        _opCtx.get(),
        nullptr,
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName(boost::none, "unittest")));
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterface>();

    // Set up the $backupCursor stage.
    const auto spec = fromjson("{$backupCursor: {}}");
    auto backupCursorStage =
        DocumentSourceBackupCursor::createFromBson(spec.firstElement(), expCtx);

    // Call `getNext()` once, to retrieve the metadata document.
    backupCursorStage->getNext();

    // Get 'filename.wt'.
    backupCursorStage->getNext();

    auto svcCtx = _opCtx->getClient()->getServiceContext();
    auto backupCursorService = static_cast<BackupCursorService*>(BackupCursorService::get(svcCtx));

    auto backupId = backupCursorService->getBackupId_forTest();

    ASSERT(backupCursorService->isFileReturnedByCursor(backupId, "filename.wt"));
    ASSERT_FALSE(backupCursorService->isFileReturnedByCursor(UUID::gen(), "filename.wt"));
    ASSERT_FALSE(backupCursorService->isFileReturnedByCursor(backupId, "notfilename.wt"));
}

}  // namespace
}  // namespace mongo
