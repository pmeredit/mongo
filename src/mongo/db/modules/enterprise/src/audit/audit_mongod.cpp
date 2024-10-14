/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_mongod.h"

#include "audit/audit_commands_gen.h"
#include "audit/audit_config_command.h"
#include "audit/audit_config_gen.h"
#include "audit/audit_manager.h"
#include "audit/audit_options_gen.h"
#include "mongo/db/audit.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/commands/set_cluster_parameter_invocation.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/generic_argument_util.h"
#include "mongo/db/s/forwardable_operation_metadata.h"
#include "mongo/logv2/log.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"
#include "mongo/s/write_ops/batched_command_response.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {
namespace audit {

namespace {
static constexpr StringData kAuditConfigParameter = "auditConfig"_sd;
}  // namespace

namespace {
struct SetAuditConfigCmd {
    using Request = SetAuditConfigCommand;
    using Reply = void;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to change audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kNever;
    static void typedRun(OperationContext* opCtx, const Request& cmd) {
        LOGV2_WARNING(6648200,
                      "Command 'setAuditConfig' is deprecated, use "
                      "setClusterParameter on the auditConfig cluster parameter instead.");

        auto docBSON = [&]() {
            AuditConfigDocument doc;
            // Since clusterParameterTime is required, we must set it before running toBSON().
            doc.setFilter(cmd.getFilter().getOwned());
            doc.setAuditAuthorizationSuccess(cmd.getAuditAuthorizationSuccess());
            doc.setClusterParameterTime(LogicalTime::kUninitialized);
            // Don't send the empty clusterParameterTime to setClusterParameter, let it generate one
            // instead.
            return doc.toBSON().filterFieldsUndotted(BSON("clusterParameterTime" << true), false);
        }();

        BSONObj setClusterParameterObj;
        if (serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer)) {
            ConfigsvrSetClusterParameter setClusterParameter(
                BSON(kAuditConfigParameter << docBSON));
            setClusterParameter.setDbName(DatabaseName::kAdmin);
            setClusterParameterObj = setClusterParameter.toBSON();
        } else {
            SetClusterParameter setClusterParameter(BSON(kAuditConfigParameter << docBSON));
            setClusterParameter.setDbName(DatabaseName::kAdmin);
            setClusterParameterObj = setClusterParameter.toBSON();
        }

        // We need to create a separate client and opCtx with internal authorization to
        // correctly run setClusterParameter. Grab client attribs and opCtx metadata to pass on
        // to the invocation of the setClusterParameter command.
        audit::ImpersonatedClientAttrs impersonatedClientAttrs(opCtx->getClient());
        ForwardableOperationMetadata forwardableOpMetadata(opCtx);

        // Allow this thread to be killable. If interrupted, runCommand will fail and the error
        // will be returned to the user.
        auto altClient = opCtx->getService()->makeClient("set-audit-config");

        AlternativeClientRegion clientRegion(altClient);
        auto altOpCtx = cc().makeOperationContext();
        auto as = AuthorizationSession::get(cc());
        as->grantInternalAuthorization();

        forwardableOpMetadata.setOn(altOpCtx.get());
        as->setImpersonatedUserData(std::move(impersonatedClientAttrs.userName),
                                    std::move(impersonatedClientAttrs.roleNames));

        DBDirectClient directClient(altOpCtx.get());

        BSONObj res;
        directClient.runCommand(DatabaseName::kAdmin, setClusterParameterObj, res);
        uassertStatusOK(getStatusFromCommandResult(res));
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<SetAuditConfigCmd>).forShard();

struct GetAuditConfigCmd {
    using Request = GetAuditConfigCommand;
    using Reply = AuditConfigDocument;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        LOGV2_WARNING(6648202,
                      "Command 'getAuditConfig' is deprecated, use "
                      "getClusterParameter on the auditConfig cluster parameter instead.");
        return getGlobalAuditManager()->getAuditConfig();
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<GetAuditConfigCmd>).forShard();

}  // namespace
}  // namespace audit
}  // namespace mongo
