/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mock_mongo_interface_for_backup_tests.h"

namespace mongo {

boost::intrusive_ptr<ExpressionContext> createMockBackupExpressionContext(
    const ServiceContext::UniqueOperationContext& opCtx) {
    return ExpressionContextBuilder{}
        .opCtx(opCtx.get())
        .ns(NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin))
        .mongoProcessInterface(std::make_unique<MockMongoInterfaceForBackupTests>())
        .build();
}
}  // namespace mongo
