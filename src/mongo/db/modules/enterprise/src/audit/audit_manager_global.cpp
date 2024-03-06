/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "audit_manager_global.h"

#include "audit_manager.h"
#include "audit_options.h"
#include "mongo/base/init.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/matcher/extensions_callback_disallow_extensions.h"
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

MONGO_INITIALIZER_WITH_PREREQUISITES(InitializeGlobalAuditManager, ("CreateAuditManager"))
(InitializerContext* context) {
    audit::getGlobalAuditManager()->enabled = auditGlobalParams.enabled;

    if (auditGlobalParams.enabled) {
        StatusWithMatchExpression parseResult = MatchExpressionParser::parse(
            auditGlobalParams.auditFilter, ExtensionsCallbackDisallowExtensions());
        if (!parseResult.isOK()) {
            return Status(ErrorCodes::BadValue, "failed to parse auditFilter");
        }
        AuditManager* am = audit::getGlobalAuditManager();
        am->auditFilter = parseResult.getValue().release();

        am->auditLogPath = auditGlobalParams.auditPath;

        am->auditFormat = auditGlobalParams.auditFormat;
    }

    return Status::OK();
}

}  // namespace audit
}  // namespace mongo
