/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional/optional.hpp>
#include <functional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"
#include "mongo/db/exec/mutable_bson/document.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/write_ops/write_ops.h"
#include "mongo/db/tenant_id.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/functional.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo::audit {


class AuthenticateEvent;
class CommandInterface;

using ActivityId = int;
using Severity = int;

struct TryLogEventParamsOCSF : public TryLogEventParams {
    TryLogEventParamsOCSF(Client* client,
                          ocsf::OCSFEventCategory eventCategory,
                          ocsf::OCSFEventClass eventClass,
                          ActivityId activityId,
                          Severity severity,
                          AuditInterface::AuditEvent::Serializer serializer,
                          ErrorCodes::Error code)
        : TryLogEventParams(client, code, serializer),
          ocsfEventCategory(eventCategory),
          ocsfEventClass(eventClass),
          activityId(activityId),
          severity(severity){};

    TryLogEventParamsOCSF(Client* client,
                          ocsf::OCSFEventCategory eventCategory,
                          ocsf::OCSFEventClass eventClass,
                          ActivityId activityId,
                          Severity severity,
                          AuditInterface::AuditEvent::Serializer serializer,
                          ErrorCodes::Error code,
                          const boost::optional<TenantId> tenantId)
        : TryLogEventParams(client, code, serializer, tenantId),
          ocsfEventCategory(eventCategory),
          ocsfEventClass(eventClass),
          activityId(activityId),
          severity(severity){};

    ocsf::OCSFEventCategory ocsfEventCategory;
    ocsf::OCSFEventClass ocsfEventClass;
    ActivityId activityId;
    Severity severity;
};

class AuditOCSF : public AuditInterface {
public:
    AuditOCSF() = default;
    ~AuditOCSF() override = default;

    void logClientMetadata(Client* client) const override;

    void logAuthentication(Client* client, const AuthenticateEvent& event) const override;

    void logCommandAuthzCheck(Client* client,
                              const OpMsgRequest& cmdObj,
                              const CommandInterface& command,
                              ErrorCodes::Error result) const override;

    void logKillCursorsAuthzCheck(Client* client,
                                  const NamespaceString& ns,
                                  long long cursorId,
                                  ErrorCodes::Error result) const override;

    void logCreateUser(Client* client,
                       const UserName& username,
                       bool password,
                       const BSONObj* customData,
                       const std::vector<RoleName>& roles,
                       const boost::optional<BSONArray>& restrictions) const override;

    void logDropUser(Client* client, const UserName& username) const override;
    void logDropAllUsersFromDatabase(Client* client, const DatabaseName& dbname) const override;

    void logUpdateUser(Client* client,
                       const UserName& username,
                       bool password,
                       const BSONObj* customData,
                       const std::vector<RoleName>* roles,
                       const boost::optional<BSONArray>& restrictions) const override;

    void logGrantRolesToUser(Client* client,
                             const UserName& username,
                             const std::vector<RoleName>& roles) const override;

    void logRevokeRolesFromUser(Client* client,
                                const UserName& username,
                                const std::vector<RoleName>& roles) const override;

    void logCreateRole(Client* client,
                       const RoleName& role,
                       const std::vector<RoleName>& roles,
                       const PrivilegeVector& privileges,
                       const boost::optional<BSONArray>& restrictions) const override;

    void logUpdateRole(Client* client,
                       const RoleName& role,
                       const std::vector<RoleName>* roles,
                       const PrivilegeVector* privileges,
                       const boost::optional<BSONArray>& restrictions) const override;

    void logDropRole(Client* client, const RoleName& role) const override;
    void logDropAllRolesFromDatabase(Client* client, const DatabaseName& dbname) const override;

    void logGrantRolesToRole(Client* client,
                             const RoleName& role,
                             const std::vector<RoleName>& roles) const override;

    void logRevokeRolesFromRole(Client* client,
                                const RoleName& role,
                                const std::vector<RoleName>& roles) const override;

    void logGrantPrivilegesToRole(Client* client,
                                  const RoleName& role,
                                  const PrivilegeVector& privileges) const override;

    void logRevokePrivilegesFromRole(Client* client,
                                     const RoleName& role,
                                     const PrivilegeVector& privileges) const override;

    void logReplSetReconfig(Client* client,
                            const BSONObj* oldConfig,
                            const BSONObj* newConfig) const override;

    void logApplicationMessage(Client* client, StringData msg) const override;

    void logStartupOptions(Client* client, const BSONObj& startupOptions) const override;

    void logShutdown(Client* client) const override;

    void logLogout(Client* client,
                   StringData reason,
                   const BSONArray& initialUsers,
                   const BSONArray& updatedUsers,
                   const boost::optional<Date_t>& loginTime) const override;

    void logCreateIndex(Client* client,
                        const BSONObj* indexSpec,
                        StringData indexname,
                        const NamespaceString& nsname,
                        StringData indexBuildState,
                        ErrorCodes::Error result) const override;

    void logCreateCollection(Client* client, const NamespaceString& nsname) const override;

    void logCreateView(Client* client,
                       const NamespaceString& nsname,
                       const NamespaceString& viewOn,
                       BSONArray pipeline,
                       ErrorCodes::Error code) const override;

    void logImportCollection(Client* client, const NamespaceString& nsname) const override;

    void logCreateDatabase(Client* client, const DatabaseName& dbname) const override;


    void logDropIndex(Client* client,
                      StringData indexname,
                      const NamespaceString& nsname) const override;

    void logDropCollection(Client* client, const NamespaceString& nsname) const override;

    void logDropView(Client* client,
                     const NamespaceString& nsname,
                     const NamespaceString& viewOn,
                     const std::vector<BSONObj>& pipeline,
                     ErrorCodes::Error code) const override;


    void logDropDatabase(Client* client, const DatabaseName& dbname) const override;

    void logRenameCollection(Client* client,
                             const NamespaceString& source,
                             const NamespaceString& target) const override;

    void logEnableSharding(Client* client, StringData dbname) const override;
    void logAddShard(Client* client, StringData name, const std::string& servers) const override;
    void logRemoveShard(Client* client, StringData shardname) const override;

    void logShardCollection(Client* client,
                            const NamespaceString& ns,
                            const BSONObj& keyPattern,
                            bool unique) const override;

    void logRefineCollectionShardKey(Client* client,
                                     const NamespaceString& ns,
                                     const BSONObj& keyPattern) const override;

    void logInsertOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc) const override;

    void logUpdateOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc) const override;

    void logRemoveOperation(Client* client,
                            const NamespaceString& nss,
                            const BSONObj& doc) const override;

    void logGetClusterParameter(Client* client,
                                const std::variant<std::string, std::vector<std::string>>&
                                    requestedParameters) const override;

    void logSetClusterParameter(Client* client,
                                const BSONObj& oldValue,
                                const BSONObj& newValue,
                                const boost::optional<TenantId>& tenantId) const override;

    void logUpdateCachedClusterParameter(Client* client,
                                         const BSONObj& oldValue,
                                         const BSONObj& newValue,
                                         const boost::optional<TenantId>& tenantId) const override;

    void logRotateLog(Client* client,
                      const Status& logStatus,
                      const std::vector<Status>& errors,
                      const std::string& suffix) const override;

    void logConfigEvent(Client* client, const AuditConfigDocument& config) const override;

    // Logs the event when data containing privileges is changed via direct access.
    void logDirectAuthOperation(Client* client,
                                const NamespaceString& nss,
                                const BSONObj& doc,
                                DirectAuthOperation operation) const;

    class AuditEventOCSF : public AuditEvent {
    public:
        using TypeArgT = TryLogEventParamsOCSF;

        AuditEventOCSF(const TryLogEventParamsOCSF& tryLogParams);

        StringData getTimestampFieldName() const override;

        static void _buildNetwork(Client* client, BSONObjBuilder* builder);

        // Build a User object into a "user" field based on an on-disk document.
        static void _buildUser(BSONObjBuilder* builder,
                               BSONObj doc,
                               const boost::optional<TenantId>& tenantId);

        // Build a User object into a "user" field based on known UserName and RoleNames.
        static void _buildUser(BSONObjBuilder* builder, const UserName& userName);
        static void _buildUser(BSONObjBuilder* builder,
                               const UserName& userName,
                               RoleNameIterator roles);
        static void _buildUser(BSONObjBuilder* builder,
                               const UserName& userName,
                               const std::vector<RoleName>& roles);

        // Build a User object into a "user" field based on user attached to the Client auth
        // session.
        static void _buildUser(BSONObjBuilder* builder, Client* client);

        static void _buildProcess(BSONObjBuilder* builder);
        static void _buildDevice(BSONObjBuilder* builder);
        static void _buildEntity(BSONObjBuilder* builder,
                                 StringData entityId,
                                 const BSONObj* data,
                                 StringData name,
                                 StringData type);

    private:
        AuditEventOCSF() = delete;
        AuditEventOCSF(const AuditEventOCSF&) = delete;
        AuditEventOCSF& operator=(const AuditEventOCSF&) = delete;

        void _init(const TryLogEventParamsOCSF& tryLogParams);

        /* TODO SERVER-78816:
            static void serializeClient(Client* client, BSONObjBuilder* builder);
        */
    };
};

#undef MONGO_LOGV2_DEFAULT_COMPONENT

}  // namespace mongo::audit
