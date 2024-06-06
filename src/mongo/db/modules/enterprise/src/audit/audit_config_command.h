/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"

namespace mongo {
namespace audit {

/**
 * Basic command template used in audit_mongod.cpp and audit_mongos.cpp
 */
template <typename Impl>
class AuditConfigCmd final : public TypedCommand<AuditConfigCmd<Impl>> {
public:
    using Request = typename Impl::Request;
    using Reply = typename Impl::Reply;
    using TC = TypedCommand<AuditConfigCmd<Impl>>;

    class Invocation final : public TC::InvocationBase {
    public:
        using TC::InvocationBase::InvocationBase;
        using TC::InvocationBase::request;

        Reply typedRun(OperationContext* opCtx) {
            return Impl::typedRun(opCtx, request());
        }

    private:
        bool supportsWriteConcern() const final {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            auto* as = AuthorizationSession::get(opCtx->getClient());
            uassert(ErrorCodes::Unauthorized,
                    Impl::kUnauthorizedMessage,
                    as->isAuthorizedForActionsOnResource(
                        ResourcePattern::forClusterResource(request().getDbName().tenantId()),
                        ActionType::auditConfigure));
        }

        NamespaceString ns() const final {
            return NamespaceString(request().getDbName());
        }
    };

    typename TC::AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return Impl::kSecondaryAllowed;
    }

    bool adminOnly() const final {
        return true;
    }
};

}  // namespace audit
}  // namespace mongo
