/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_client_observer.h"

#include "audit/audit_manager.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/rpc/metadata/audit_client_attrs.h"
#include "mongo/rpc/metadata/audit_user_attrs.h"

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

    if (AuthorizationSession::exists(client)) {
        auto session = AuthorizationSession::get(client);

        auto userName = session->getAuthenticatedUserName();
        auto roleNameContainer = roleNameIteratorToContainer<std::vector<RoleName>>(
            session->getAuthenticatedRoleNames());
        if (userName) {
            rpc::AuditUserAttrs::set(opCtx,
                                     rpc::AuditUserAttrs(*userName, std::move(roleNameContainer)));
        }
    }
}

void AuditClientObserver::onCreateClient(Client* client) {
    auto session = client->session();
    if (!session) {
        return;
    }

    auto local = session->local();
    auto remote = session->getSourceRemoteEndpoint();
    std::vector<HostAndPort> proxies;
    if (auto proxyEndpoint = session->getProxiedDstEndpoint()) {
        proxies.push_back(*proxyEndpoint);
    }

    rpc::AuditClientAttrs::set(
        client, rpc::AuditClientAttrs(std::move(local), std::move(remote), std::move(proxies)));
}

}  // namespace mongo::audit
