/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include "audit/audit_mongod.h"

#include "audit/audit_commands_gen.h"
#include "audit/audit_config_command.h"
#include "audit/audit_config_gen.h"
#include "audit/audit_manager.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
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

namespace {
struct SetAuditConfigCmd {
    using Request = SetAuditConfigCommand;
    using Reply = void;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to change audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kNever;
    static void typedRun(OperationContext* opCtx, const Request& cmd) {
        auto* am = getGlobalAuditManager();
        uassertSetAuditConfigAllowed(am);

        uassert(ErrorCodes::NotImplemented,
                "Command 'setAuditConfig' is not supported on shard servers",
                serverGlobalParams.clusterRole != ClusterRole::ShardServer);

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
};
AuditConfigCmd<SetAuditConfigCmd> setAuditConfigCmd;

struct GetAuditConfigGenerationCmd {
    using Request = GetAuditConfigGenerationCommand;
    using Reply = GetAuditConfigGenerationReply;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        return Reply(getGlobalAuditManager()->getConfigGeneration());
    }
};
AuditConfigCmd<GetAuditConfigGenerationCmd> getAuditConfigGenerationCmd;

struct GetAuditConfigCmd {
    using Request = GetAuditConfigCommand;
    using Reply = AuditConfigDocument;
    static constexpr StringData kUnauthorizedMessage =
        "Not authorized to read audit configuration"_sd;
    static constexpr auto kSecondaryAllowed = BasicCommand::AllowedOnSecondary::kAlways;
    static Reply typedRun(OperationContext* opCtx, const Request& cmd) {
        return getGlobalAuditManager()->getAuditConfig();
    }
};
AuditConfigCmd<GetAuditConfigCmd> getAuditConfigCmd;

}  // namespace
}  // namespace audit
}  // namespace mongo
