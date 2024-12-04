/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_client_attrs.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/decorable.h"

namespace mongo::audit {

namespace {
const auto getAuditClientAttrs =
    OperationContext::declareDecoration<std::unique_ptr<AuditClientAttrs>>();

class AuditClientObserver final : public ServiceContext::ClientObserver {
public:
    void onCreateClient(Client* client) final{};
    void onDestroyClient(Client* client) final{};

    void onCreateOperationContext(OperationContext* opCtx) final {
        auto client = opCtx->getClient();

        if (!client) {
            return;
        }

        auto session = AuthorizationSession::get(client);

        auto username = session->getAuthenticatedUserName();
        auto rolenameContainer = roleNameIteratorToContainer<std::vector<RoleName>>(
            session->getAuthenticatedRoleNames());

        auto attrs =
            std::make_unique<AuditClientAttrs>(std::move(username), std::move(rolenameContainer));
        AuditClientAttrs::set(opCtx, std::move(attrs));
    };

    void onDestroyOperationContext(OperationContext* opCtx) final{};
};

ServiceContext::ConstructorActionRegisterer auditClientObserverRegisterer{
    "AuditClientObserverRegisterer", [](ServiceContext* svCtx) {
        svCtx->registerClientObserver(std::make_unique<AuditClientObserver>());
    }};

}  // namespace

AuditClientAttrs* AuditClientAttrs::get(OperationContext* opCtx) {
    return getAuditClientAttrs(opCtx).get();
}

void AuditClientAttrs::set(OperationContext* opCtx, std::unique_ptr<AuditClientAttrs> attrs) {
    getAuditClientAttrs(opCtx) = std::move(attrs);
}

}  // namespace mongo::audit
