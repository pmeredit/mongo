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
    "AuditClientObserverRegisterer", {"InitializeGlobalAuditManager"}, [](ServiceContext* svcCtx) {
        if (getGlobalAuditManager()->isEnabled()) {
            svcCtx->registerClientObserver(std::make_unique<AuditClientObserver>());
        }
    }};

}  // namespace

void AuditClientObserver::onCreateClient(Client* client) {
    rpc::AuditClientAttrs::resetToPeerClient(client);
}

}  // namespace mongo::audit
