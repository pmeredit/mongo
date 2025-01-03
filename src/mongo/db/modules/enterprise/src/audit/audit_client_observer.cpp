/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_client_observer.h"
#include "audit/audit_manager.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/rpc/metadata/audit_user_attrs.h"
#include "mongo/util/decorable.h"

namespace mongo::audit {
namespace {
ServiceContext::ConstructorActionRegisterer auditClientObserverRegisterer{
    "AuditClientObserverRegisterer", {"InitializeGlobalAuditManager"}, [](ServiceContext* svCtx) {
        if (getGlobalAuditManager()->isEnabled()) {
            svCtx->registerClientObserver(std::make_unique<AuditClientObserver>());
        }
    }};

}  // namespace

void AuditClientObserver::onCreateOperationContext(OperationContext* opCtx) {
    auto client = opCtx->getClient();

    if (!client) {
        return;
    }

    if (!AuthorizationSession::exists(client)) {
        rpc::AuditUserAttrs::set(
            opCtx, std::make_unique<rpc::AuditUserAttrs>(boost::none, std::vector<RoleName>()));
        return;
    }

    auto session = AuthorizationSession::get(client);

    auto username = session->getAuthenticatedUserName();
    auto rolenameContainer =
        roleNameIteratorToContainer<std::vector<RoleName>>(session->getAuthenticatedRoleNames());

    rpc::AuditUserAttrs::set(
        opCtx,
        std::make_unique<rpc::AuditUserAttrs>(std::move(username), std::move(rolenameContainer)));
}

}  // namespace mongo::audit
