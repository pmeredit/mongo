/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "sasl/idp_manager.h"
#include "sasl/oidc_commands_gen.h"

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"

namespace mongo::auth {
namespace {
class CmdOIDCRefreshKeys final : public TypedCommand<CmdOIDCRefreshKeys> {
public:
    using Request = OIDCRefreshKeysCommand;

    std::string help() const override {
        return "An enterprise-only command to force an immediate refresh JSON Web Keys for the "
               "requested identity providers on this node";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool adminOnly() const override {
        return true;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        void typedRun(OperationContext* opCtx) {
            uassert(ErrorCodes::IllegalOperation,
                    "OIDC is not enabled on this node",
                    IDPManager::isOIDCEnabled());
            auto* idpManager = IDPManager::get();

            const auto& requestedInvalidateOnFailure = request().getInvalidateOnFailure();
            const auto& requestedIdentityProviders = request().getIdentityProviders();
            if (requestedIdentityProviders) {
                std::set<StringData> requestedIdPs(requestedIdentityProviders->begin(),
                                                   requestedIdentityProviders->end());
                uassertStatusOK(idpManager->refreshIDPs(opCtx,
                                                        requestedIdPs,
                                                        IDPJWKSRefresher::RefreshOption::kNow,
                                                        requestedInvalidateOnFailure));
            } else {
                uassertStatusOK(idpManager->refreshAllIDPs(
                    opCtx, IDPJWKSRefresher::RefreshOption::kNow, requestedInvalidateOnFailure));
            }
        }

    private:
        bool supportsWriteConcern() const override {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            auto* authzSession = AuthorizationSession::get(opCtx->getClient());
            uassert(ErrorCodes::Unauthorized,
                    "Not authorized to retrieve cluster server parameters",
                    authzSession->isAuthorizedForPrivilege(Privilege{
                        ResourcePattern::forClusterResource(request().getDbName().tenantId()),
                        ActionType::oidcRefreshKeys}));
        }

        NamespaceString ns() const override {
            return NamespaceString(request().getDbName());
        }
    };
};
MONGO_REGISTER_COMMAND(CmdOIDCRefreshKeys).forShard().forRouter();

class CmdOIDCListKeys final : public TypedCommand<CmdOIDCListKeys> {
public:
    using Request = OIDCListKeysCommand;
    using Reply = OIDCListKeysCommand::Reply;

    std::string help() const override {
        return "An enterprise-only command to list the JWKS for the requested IdPs on this node";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool adminOnly() const override {
        return true;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        Reply typedRun(OperationContext* opCtx) {
            uassert(ErrorCodes::IllegalOperation,
                    "OIDC is not enabled on this node",
                    IDPManager::isOIDCEnabled());
            auto* idpManager = IDPManager::get();

            const auto& requestedIdentityProviders = request().getIdentityProviders();
            BSONObjBuilder bob;
            if (requestedIdentityProviders) {
                std::set<StringData> requestedIdPs(requestedIdentityProviders->begin(),
                                                   requestedIdentityProviders->end());
                idpManager->serializeJWKSets(&bob, requestedIdPs);
            } else {
                idpManager->serializeJWKSets(&bob, boost::none);
            }

            auto requestedKeys = bob.obj();
            return Reply(requestedKeys);
        }

    private:
        bool supportsWriteConcern() const override {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            auto* authzSession = AuthorizationSession::get(opCtx->getClient());
            uassert(ErrorCodes::Unauthorized,
                    "Not authorized to retrieve cluster server parameters",
                    authzSession->isAuthorizedForPrivilege(Privilege{
                        ResourcePattern::forClusterResource(request().getDbName().tenantId()),
                        ActionType::oidcListKeys}));
        }

        NamespaceString ns() const override {
            return NamespaceString(request().getDbName());
        }
    };
};
MONGO_REGISTER_COMMAND(CmdOIDCListKeys).forShard().forRouter();

}  // namespace
}  // namespace mongo::auth
