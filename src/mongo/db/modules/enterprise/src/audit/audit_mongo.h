/**
 *    Copyright (C) 2023 10gen Inc.
 */

#include <boost/optional/optional.hpp>
#include <functional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/audit.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/client.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/ops/write_ops.h"
#include "mongo/db/service_context.h"
#include "mongo/db/tenant_id.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/util/functional.h"


namespace mongo::audit {

class AuditMongo : public AuditInterface {
public:
    AuditMongo() = default;
    virtual ~AuditMongo() = default;

    void logAuthentication(Client* client, const AuthenticateEvent& event) const override;

    void logClientMetadata(Client* client) const override;

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
                   const BSONArray& updatedUsers) const override;

    void logCreateIndex(Client* client,
                        const BSONObj* indexSpec,
                        StringData indexname,
                        const NamespaceString& nsname,
                        StringData indexBuildState,
                        ErrorCodes::Error result) const override;


    void logCreateCollection(Client* client, const NamespaceString& nsname) const override;

    void logCreateView(Client* client,
                       const NamespaceString& nsname,
                       StringData viewOn,
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
                     StringData viewOn,
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
                            StringData ns,
                            const BSONObj& keyPattern,
                            bool unique) const override;

    void logRefineCollectionShardKey(Client* client,
                                     StringData ns,
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
                                const stdx::variant<std::string, std::vector<std::string>>&
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
};

}  // namespace mongo::audit
