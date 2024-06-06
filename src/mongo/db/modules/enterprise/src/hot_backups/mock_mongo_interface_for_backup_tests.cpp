/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mock_mongo_interface_for_backup_tests.h"

namespace mongo {

boost::intrusive_ptr<ExpressionContext> createMockBackupExpressionContext(
    const ServiceContext::UniqueOperationContext& opCtx) {
    auto expCtx = make_intrusive<ExpressionContext>(
        opCtx.get(),
        nullptr,
        NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin));
    expCtx->mongoProcessInterface = std::make_unique<MockMongoInterfaceForBackupTests>();
    return expCtx;
}
}  // namespace mongo
