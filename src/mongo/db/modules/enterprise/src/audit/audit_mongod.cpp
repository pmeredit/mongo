/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit/audit_mongod.h"

#include "audit/audit_commands_gen.h"
#include "audit/audit_config_command.h"
#include "audit/audit_config_gen.h"
#include "audit/audit_manager.h"
#include "audit/audit_options_gen.h"
#include "mongo/db/audit.h"
#include "mongo/db/commands/set_cluster_parameter_invocation.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/s/forwardable_operation_metadata.h"
#include "mongo/logv2/log.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"
#include "mongo/s/write_ops/batched_command_response.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {
namespace audit {

namespace {
void uassertSetAuditConfigAllowed(AuditManager* am) {
    auto role = serverGlobalParams.clusterRole;
    uassert(ErrorCodes::ErrorCodes::NotImplemented,
            "setAuditConfig is not implemented on data bearing shards",
            role.has(ClusterRole::None) || role.has(ClusterRole::ConfigServer));

    uassert(ErrorCodes::AuditingNotEnabled, "Auditing is not enabled", am->isEnabled());
    uassert(ErrorCodes::RuntimeAuditConfigurationNotEnabled,
            "Runtime audit configuration has not been enabled",
            am->getRuntimeConfiguration());
}

static constexpr StringData kAuditConfigParameter = "auditConfig"_sd;
const auto kMajorityWriteConcern = BSON("writeConcern" << BSON("w"
                                                               << "majority"));
}  // namespace

void upsertConfig(OperationContext* opCtx, const AuditConfigDocument& doc) {
    constexpr auto kFailureMessage = "Failed updating audit configuration"_sd;

    // No config doc yet: Insert it (basic upsert)
    // Config document already with different generation: Update it (typical case)
    // Config document already exists with same/newer generation: Conflict occured, upsert will
    // fail.
    auto query = BSON("_id" << kAuditDocID << "generation" << BSON("$ne" << *doc.getGeneration()));
    auto update = ([&] {
        BSONObjBuilder updateBuilder;
        BSONObjBuilder set(updateBuilder.subobjStart("$set"));
        doc.serialize(&set);
        set.doneFast();
        return updateBuilder.obj();
    })();

    BSONObj res;
    try {
        DBDirectClient client(opCtx);

        client.runCommand(
            DatabaseName::kConfig,
            [&] {
                write_ops::UpdateCommandRequest updateOp(NamespaceString::kConfigSettingsNamespace);
                updateOp.setUpdates({[&] {
                    write_ops::UpdateOpEntry entry;
                    entry.setQ(query);
                    entry.setU(write_ops::UpdateModification::parseFromClassicUpdate(update));
                    entry.setMulti(false);
                    entry.setUpsert(true);
                    return entry;
                }()});
                return updateOp.toBSON(kMajorityWriteConcern);
            }(),
            res);
    } catch (const DBException& ex) {
        uassertStatusOK(ex.toStatus().withContext(kFailureMessage));
    }

    BatchedCommandResponse response;
    std::string errmsg;
    if (!response.parseBSON(res, &errmsg)) {
        uasserted(ErrorCodes::FailedToParse, str::stream() << kFailureMessage << ": " << errmsg);
    }

    uassertStatusOK(response.toStatus());
    uassert(ErrorCodes::OperationFailed, kFailureMessage, response.getN());
}

namespace {
struct SetAuditConfigCmd {
    using Request = SetAuditConfigCommand;
    using Reply = void;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to change audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kNever;
    static void typedRun(OperationContext* opCtx, const Request& cmd) {
        if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            // Enabled and FCV is high enough, yell about deprecation and redirect to
            // setClusterParameter
            LOGV2_WARNING(6648200,
                          "Command 'setAuditConfig' is deprecated, use "
                          "setClusterParameter on the auditConfig cluster parameter instead.");
            AuditConfigDocument doc;
            doc.setFilter(cmd.getFilter().getOwned());
            doc.setAuditAuthorizationSuccess(cmd.getAuditAuthorizationSuccess());

            BSONObj setClusterParameterObj;
            if (serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer)) {
                ConfigsvrSetClusterParameter setClusterParameter(
                    BSON(kAuditConfigParameter << doc.toBSON()));
                setClusterParameter.setDbName(DatabaseName::kAdmin);
                setClusterParameterObj = setClusterParameter.toBSON({});
            } else {
                SetClusterParameter setClusterParameter(
                    BSON(kAuditConfigParameter << doc.toBSON()));
                setClusterParameter.setDbName(DatabaseName::kAdmin);
                setClusterParameterObj = setClusterParameter.toBSON({});
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
            as->grantInternalAuthorization(altOpCtx.get());

            forwardableOpMetadata.setOn(altOpCtx.get());
            as->setImpersonatedUserData(std::move(impersonatedClientAttrs.userName),
                                        std::move(impersonatedClientAttrs.roleNames));

            DBDirectClient directClient(altOpCtx.get());

            BSONObj res;
            directClient.runCommand(DatabaseName::kAdmin, setClusterParameterObj, res);
            uassertStatusOK(getStatusFromCommandResult(res));
            // (Ignore FCV check): This is intentional to just check if feature flag is enabled.
        } else if (feature_flags::gFeatureFlagAuditConfigClusterParameter
                       .isEnabledAndIgnoreFCVUnsafe()) {
            // Enabled but FCV is not high enough, fail because audit config is read only in this
            // case
            uasserted(ErrorCodes::CommandNotSupported,
                      "Command 'setAuditConfig' cannot be run while in a downgraded FCV state "
                      "while featureFlagAuditConfigClusterParameter is enabled. Please complete or "
                      "abort the downgrade and try again.");
        } else {
            // Disabled, run old version of setAuditConfig
            auto* am = getGlobalAuditManager();
            uassertSetAuditConfigAllowed(am);

            // Validate that this filter is legal before attempting to update.
            auto filterBSON = cmd.getFilter().getOwned();
            am->parseFilter(filterBSON);

            AuditConfigDocument doc;
            doc.set_id(kAuditDocID);
            doc.setFilter(filterBSON);
            doc.setAuditAuthorizationSuccess(cmd.getAuditAuthorizationSuccess());
            doc.setGeneration(OID::gen());

            // Auditing of the config change and update of AuditManager occurs in AuditOpObserver.
            upsertConfig(opCtx, doc);
        }
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<SetAuditConfigCmd>).forShard();

struct GetAuditConfigGenerationCmd {
    using Request = GetAuditConfigGenerationCommand;
    using Reply = GetAuditConfigGenerationReply;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            uasserted(ErrorCodes::APIDeprecationError, "Audit generation is deprecated");
        } else {
            return Reply(getGlobalAuditManager()->getConfigGeneration());
        }
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<GetAuditConfigGenerationCmd>).forShard();

struct GetAuditConfigCmd {
    using Request = GetAuditConfigCommand;
    using Reply = AuditConfigDocument;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        if (feature_flags::gFeatureFlagAuditConfigClusterParameter.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            LOGV2_WARNING(6648202,
                          "Command 'getAuditConfig' is deprecated, use "
                          "getClusterParameter on the auditConfig cluster parameter instead.");
        }
        return getGlobalAuditManager()->getAuditConfig();
    }
};
MONGO_REGISTER_COMMAND(AuditConfigCmd<GetAuditConfigCmd>).forShard();

}  // namespace
}  // namespace audit
}  // namespace mongo
