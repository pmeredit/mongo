/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_client_attrs.h"

#include "audit/audit_manager.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/decorable.h"

namespace mongo::audit {

namespace {
const auto getAuditUserAttrs =
    OperationContext::declareDecoration<std::unique_ptr<AuditUserAttrs>>();

ServiceContext::ConstructorActionRegisterer auditClientObserverRegisterer{
    "AuditClientObserverRegisterer", {"InitializeGlobalAuditManager"}, [](ServiceContext* svCtx) {
        if (getGlobalAuditManager()->isEnabled()) {
            svCtx->registerClientObserver(std::make_unique<AuditClientObserver>());
        }
    }};

}  // namespace

AuditUserAttrs* AuditUserAttrs::get(OperationContext* opCtx) {
    return getAuditUserAttrs(opCtx).get();
}

void AuditUserAttrs::set(OperationContext* opCtx, std::unique_ptr<AuditUserAttrs> attrs) {
    getAuditUserAttrs(opCtx) = std::move(attrs);
}

void AuditClientObserver::onCreateOperationContext(OperationContext* opCtx) {
    auto client = opCtx->getClient();

    if (!client) {
        return;
    }

    auto session = AuthorizationSession::get(client);

    auto username = session->getAuthenticatedUserName();
    auto rolenameContainer =
        roleNameIteratorToContainer<std::vector<RoleName>>(session->getAuthenticatedRoleNames());

    auto attrs =
        std::make_unique<AuditUserAttrs>(std::move(username), std::move(rolenameContainer));
    AuditUserAttrs::set(opCtx, std::move(attrs));
}

}  // namespace mongo::audit
