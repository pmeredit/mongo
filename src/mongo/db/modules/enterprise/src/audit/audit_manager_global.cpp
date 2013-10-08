/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_manager_global.h"

#include "audit_manager.h"
#include "mongo/base/init.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace audit {

namespace {
    AuditManager* globalAuditManager = NULL;
}

    void setGlobalAuditManager(AuditManager* auditManager) {
        fassert(17186, globalAuditManager == NULL);
        globalAuditManager = auditManager;
    }

    void clearGlobalAuditManager() {
        fassert(17187, globalAuditManager != NULL);
        globalAuditManager = NULL;
    }

    AuditManager* getGlobalAuditManager() {
        fassert(17188, globalAuditManager != NULL);
        return globalAuditManager;
    }


    MONGO_INITIALIZER(CreateAuditManager)(InitializerContext* context) {
        setGlobalAuditManager(new AuditManager());
        return Status::OK();
    }

} // namespace audit
} // namespace mongo
