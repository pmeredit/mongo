/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <memory>

#include "backup_cursor_service.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/process_interface/stub_mongo_process_interface.h"

namespace mongo {

/**
 * A MongoProcessInterface used for testing that directs DocumentSourceBackupCursor to open a
 * backup cursor through BackupCursorService.
 */
class MockMongoInterfaceForBackupTests final : public StubMongoProcessInterface {
public:
    BackupCursorState openBackupCursor(OperationContext* opCtx,
                                       const StorageEngine::BackupOptions& options) override {
        auto svcCtx = opCtx->getClient()->getServiceContext();
        auto backupCursorService =
            static_cast<BackupCursorService*>(BackupCursorService::get(svcCtx));
        return backupCursorService->openBackupCursor(opCtx, options);
    }

    void closeBackupCursor(OperationContext* opCtx, const UUID& backupId) override {
        auto svcCtx = opCtx->getClient()->getServiceContext();
        auto backupCursorService =
            static_cast<BackupCursorService*>(BackupCursorService::get(svcCtx));
        return backupCursorService->closeBackupCursor(opCtx, backupId);
    }
};

/**
 * Sets up a new ExpressionContext, using a MockMongoInterfaceForBackupTests for the
 * mongoProcessInterface.
 */
boost::intrusive_ptr<ExpressionContext> createMockBackupExpressionContext(
    const ServiceContext::UniqueOperationContext& opCtx);

}  // namespace mongo
