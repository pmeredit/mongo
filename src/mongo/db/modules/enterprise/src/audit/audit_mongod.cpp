/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "audit_mongod.h"

#include "audit/audit_commands_gen.h"
#include "audit/audit_config_gen.h"
#include "audit_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/commands.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/s/write_ops/batched_command_response.h"

namespace mongo {
namespace audit {
const NamespaceString kSettingsNS(kConfigDB, kSettingsCollection);

namespace {
void uassertSetAuditConfigAllowed(AuditManager* am) {
    auto role = serverGlobalParams.clusterRole;
    uassert(ErrorCodes::ErrorCodes::NotImplemented,
            "setAuditConfig is not implemented on data bearing shards",
            (role == ClusterRole::None) || (role == ClusterRole::ConfigServer));

    uassert(ErrorCodes::AuditingNotEnabled, "Auditing is not enabled", am->isEnabled());
    uassert(ErrorCodes::RuntimeAuditConfigurationNotEnabled,
            "Runtime audit configuration has not been enabled",
            am->getRuntimeConfiguration());
}

const auto kMajorityWriteConcern = BSON("writeConcern" << BSON("w"
                                                               << "majority"));
}  // namespace

void upsertConfig(OperationContext* opCtx, const AuditConfigDocument& doc) {
    constexpr auto kFailureMessage = "Failed updating audit configuration"_sd;

    // No config doc yet: Insert it (basic upsert)
    // Config document already with different generation: Update it (typical case)
    // Config document already exists with same/newer generation: Conflict occured, upsert will
    // fail.
    auto query = BSON("_id" << kAuditDocID << "generation" << BSON("$ne" << doc.getGeneration()));
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
            kConfigDB.toString(),
            [&] {
                write_ops::UpdateCommandRequest updateOp(kSettingsNS);
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

class SetAuditConfigCmd : public TypedCommand<SetAuditConfigCmd> {
public:
    using Request = SetAuditConfigCommand;

    class Invocation : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        void typedRun(OperationContext* opCtx) {
            auto* am = getGlobalAuditManager();
            uassertSetAuditConfigAllowed(am);

            uassert(ErrorCodes::NotImplemented,
                    "Command 'setAuditConfig' is not supported on shard servers",
                    serverGlobalParams.clusterRole != ClusterRole::ShardServer);

            const auto& cmd = request();

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

    private:
        bool supportsWriteConcern() const final {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            auto* as = AuthorizationSession::get(opCtx->getClient());
            uassert(ErrorCodes::Unauthorized,
                    "Not authorized to change audit configuration",
                    as->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                         ActionType::auditConfigure));
        }

        NamespaceString ns() const final {
            return NamespaceString(NamespaceString::kAdminDb, "");
        }
    };

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const final {
        return true;
    }

} setAuditConfigCmd;

}  // namespace audit
}  // namespace mongo
