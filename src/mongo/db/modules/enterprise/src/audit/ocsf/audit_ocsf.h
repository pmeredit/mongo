/**
 *    Copyright (C) 2023 MongoDB Inc.
 */

#pragma once

#include <boost/optional/optional.hpp>
#include <functional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "audit/audit_manager.h"
#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/ops/write_ops.h"
#include "mongo/db/tenant_id.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/functional.h"

#include "audit/ocsf/ocsf_audit_events_gen.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo::audit {


class AuthenticateEvent;
class CommandInterface;


class AuditOCSF : public AuditInterface {
public:
    AuditOCSF() = default;
    ~AuditOCSF() = default;

    // TODO SERVER-78816: Move definitions to own cpp file
    // TODO SERVER-78822: Move definitions to own cpp file
    // TODO SERVER-78823: Move definitions to own cpp file
    // TODO SERVER-78824: Move definitions to own cpp file
    // TODO SERVER-78825: Move definitions to own cpp file
    // TODO SERVER-78826: Move definitions to own cpp file
    // TODO SERVER-78827: Move definitions to own cpp file

    void logClientMetadata(Client* client) const override {
        LOGV2(7881501, "AuditOCSF::logClientMetadata");
    }

    void logAuthentication(Client* client, const AuthenticateEvent& event) const override {
        LOGV2(7881502, "AuditOCSF::logAuthentication");
    }

    void logCommandAuthzCheck(Client* client,
                              const OpMsgRequest& cmdObj,
                              const CommandInterface& command,
                              ErrorCodes::Error result) const override {
        LOGV2(7881503, "AuditOCSF::logCommandAuthzCheck");
    }

    void logKillCursorsAuthzCheck(Client* client,
                                  const NamespaceString& ns,
                                  long long cursorId,
                                  ErrorCodes::Error result) const override {
        LOGV2(7881504, "AuditOCSF::logKillCursorsAuthzCheck");
    }

    void logCreateUser(Client* client,
                       const UserName& username,
                       bool password,
                       const BSONObj* customData,
                       const std::vector<RoleName>& roles,
                       const boost::optional<BSONArray>& restrictions) const override {
        LOGV2(7881505, "AuditOCSF::logCreateUser");
    }

    void logDropUser(Client* client, const UserName& username) const override {
        LOGV2(7881506, "AuditOCSF::logDropUser");
    }

    void logDropAllUsersFromDatabase(Client* client, const DatabaseName& dbname) const override {
        LOGV2(7881507, "AuditOCSF::logDropAllUsersFromDatabase");
    }

    void logUpdateUser(Client* client,
                       const UserName& username,
                       bool password,
                       const BSONObj* customData,
                       const std::vector<RoleName>* roles,
                       const boost::optional<BSONArray>& restrictions) const override {
        LOGV2(7881508, "AuditOCSF::logUpdateUser");
    }

    void logGrantRolesToUser(Client* client,
                             const UserName& username,
                             const std::vector<RoleName>& roles) const override {
        LOGV2(7881509, "AuditOCSF::logGrantRolesToUser");
    }

    void logRevokeRolesFromUser(Client* client,
                                const UserName& username,
                                const std::vector<RoleName>& roles) const override {
        LOGV2(7881510, "AuditOCSF::logRevokeRolesFromUser");
    }

    void logCreateRole(Client* client,
                       const RoleName& role,
                       const std::vector<RoleName>& roles,
                       const PrivilegeVector& privileges,
                       const boost::optional<BSONArray>& restrictions) const override {
        LOGV2(7881511, "AuditOCSF::logCreateRole");
    }

    void logUpdateRole(Client* client,
                       const RoleName& role,
                       const std::vector<RoleName>* roles,
                       const PrivilegeVector* privileges,
                       const boost::optional<BSONArray>& restrictions) const override {
        LOGV2(7881512, "AuditOCSF::logUpdateRole");
    }

    void logDropRole(Client* client, const RoleName& role) const override {
        LOGV2(7881513, "AuditOCSF::logDropRole");
    }

    void logDropAllRolesFromDatabase(Client* client, const DatabaseName& dbname) const override {
        LOGV2(7881514, "AuditOCSF::logDropAllRolesFromDatabase");
    }

    void logGrantRolesToRole(Client* client,
                             const RoleName& role,
                             const std::vector<RoleName>& roles) const override {
        LOGV2(7881515, "AuditOCSF::logGrantRolesToRole");
    }

    void logRevokeRolesFromRole(Client* client,
                                const RoleName& role,
                                const std::vector<RoleName>& roles) const override {
        LOGV2(7881516, "AuditOCSF::logRevokeRolesFromRole");
    }

    void logGrantPrivilegesToRole(Client* client,
                                  const RoleName& role,
                                  const PrivilegeVector& privileges) const override {
        LOGV2(7881517, "AuditOCSF::logGrantPrivilegesToRole");
    }

    void logRevokePrivilegesFromRole(Client* client,
                                     const RoleName& role,
                                     const PrivilegeVector& privileges) const override {
        LOGV2(7881518, "AuditOCSF::logRevokePrivilegesFromRole");
    }

    void logReplSetReconfig(Client* client,
                            const BSONObj* oldConfig,
                            const BSONObj* newConfig) const override {
        LOGV2(7881519, "AuditOCSF::logReplSetReconfig");
    }

    void logApplicationMessage(Client* client, StringData msg) const override {
        LOGV2(7881520, "AuditOCSF::logApplicationMessage");
    }

    void logStartupOptions(Client* client, const BSONObj& startupOptions) const override {
        LOGV2(7881521, "AuditOCSF::logStartupOptions");
    }

    void logShutdown(Client* client) const override {
        LOGV2(7881522, "AuditOCSF::logShutdown");
    }

    void logLogout(Client* client,
                   StringData reason,
                   const BSONArray& initialUsers,
                   const BSONArray& updatedUsers) const override {
        LOGV2(7881523, "AuditOCSF::logLogout");
    }

    void logCreateIndex(Client* client,
                        const BSONObj* indexSpec,
                        StringData indexname,
                        const NamespaceString& nsname,
                        StringData indexBuildState,
                        ErrorCodes::Error result) const override {
        LOGV2(7881524, "AuditOCSF::logCreateIndex");
    }

    void logCreateCollection(Client* client, const NamespaceString& nsname) const override {
        LOGV2(7881525, "AuditOCSF::logCreateCollection");
    }

    void logCreateView(Client* client,
                       const NamespaceString& nsname,
                       StringData viewOn,
                       BSONArray pipeline,
                       ErrorCodes::Error code) const override {
        LOGV2(7881526, "AuditOCSF::logCreateView");
    }

    void logImportCollection(Client* client, const NamespaceString& nsname) const override {
        LOGV2(7881527, "AuditOCSF::logImportCollection");
    }

    void logCreateDatabase(Client* client, const DatabaseName& dbname) const override {
        LOGV2(7881528, "AuditOCSF::logCreateDatabase");
    }


    void logDropIndex(Client* client,
                      StringData indexname,
                      const NamespaceString& nsname) const override {
        LOGV2(7881529, "AuditOCSF::logDropIndex");
    }

    void logDropCollection(Client* client, const NamespaceString& nsname) const override {
        LOGV2(7881530, "AuditOCSF::logDropCollection");
    }

    void logDropView(Client* client,
                     const NamespaceString& nsname,
                     StringData viewOn,
                     const std::vector<BSONObj>& pipeline,
                     ErrorCodes::Error code) const override {
        LOGV2(7881531, "AuditOCSF::logDropView");
    }

    void logDropDatabase(Client* client, const DatabaseName& dbname) const override {
        LOGV2(7881532, "AuditOCSF::logDropDatabase");
    }

    void logRenameCollection(Client* client,
                             const NamespaceString& source,
                             const NamespaceString& target) const override {
        LOGV2(7881533, "AuditOCSF::logRenameCollection");
    }

    void logEnableSharding(Client* client, StringData dbname) const override {
        LOGV2(7881534, "AuditOCSF::logEnableSharding");
    }

    void logAddShard(Client* client, StringData name, const std::string& servers) const override {
        LOGV2(7881535, "AuditOCSF::logAddShard");
    }

    void logRemoveShard(Client* client, StringData shardname) const override {
        LOGV2(7881536, "AuditOCSF::logRemoveShard");
    }

    void logShardCollection(Client* client,
                            StringData ns,
                            const BSONObj& keyPattern,
                            bool unique) const override {
        LOGV2(7881537, "AuditOCSF::logShardCollection");
    }

    void logRefineCollectionShardKey(Client* client,
                                     StringData ns,
                                     const BSONObj& keyPattern) const override {
        LOGV2(7881538, "AuditOCSF::logRefineCollectionShardKey");
    }

    void logInsertOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc) const override {
        LOGV2(7881539, "AuditOCSF::logInsertOperation");
    }

    void logUpdateOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc) const override {
        LOGV2(7881540, "AuditOCSF::logUpdateOperation");
    }

    void logRemoveOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc) const override {
        LOGV2(7881541, "AuditOCSF::logRemoveOperation");
    }

    void logGetClusterParameter(Client* client,
                                const stdx::variant<std::string, std::vector<std::string>>&
                                    requestedParameters) const override {
        LOGV2(7881542, "AuditOCSF::logGetClusterParameter");
    }

    void logSetClusterParameter(Client* client,
                                const BSONObj& oldValue,
                                const BSONObj& newValue,
                                const boost::optional<TenantId>& tenantId) const override {
        LOGV2(7881543, "AuditOCSF::logSetClusterParameter");
    }

    void logUpdateCachedClusterParameter(Client* client,
                                         const BSONObj& oldValue,
                                         const BSONObj& newValue,
                                         const boost::optional<TenantId>& tenantId) const override {
        LOGV2(7881544, "AuditOCSF::logUpdateCachedClusterParameter");
    }

    void logRotateLog(Client* client,
                      const Status& logStatus,
                      const std::vector<Status>& errors,
                      const std::string& suffix) const override {
        LOGV2(7881545, "AuditOCSF::logRotateLog");
    }

    void logConfigEvent(Client* client, const AuditConfigDocument& config) const override {
        LOGV2(7881546, "AuditOCSF::logConfigEvent");
    }

    class AuditEventOCSF : public AuditEvent {
    public:
        using TypeArgT = std::pair<ocsf::OCSFEventCategory, ocsf::OCSFEventClass>;

        AuditEventOCSF(Client* client,
                       TypeArgT type,
                       Serializer serializer = nullptr,
                       ErrorCodes::Error result = ErrorCodes::OK);
        AuditEventOCSF(Client* client,
                       TypeArgT type,
                       Serializer serializer,
                       ErrorCodes::Error result,
                       const boost::optional<TenantId>& tenantId);

        StringData getTimestampFieldName() const override;

    private:
        AuditEventOCSF() = delete;
        AuditEventOCSF(const AuditEventOCSF&) = delete;
        AuditEventOCSF& operator=(const AuditEventOCSF&) = delete;

        /* TODO SERVER-78816:
            void _init(Client* client,
                TypeArgT type,
                Serializer serializer,
                ErrorCodes::Error result,
                const boost::optional<TenantId>& tenantId);

            static void serializeClient(Client* client, BSONObjBuilder* builder);
        */
    };
};

#undef MONGO_LOGV2_DEFAULT_COMPONENT

}  // namespace mongo::audit
