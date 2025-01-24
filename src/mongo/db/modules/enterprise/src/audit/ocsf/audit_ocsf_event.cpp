/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/ocsf/audit_ocsf.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/multitenancy.h"
#include "mongo/rpc/metadata/audit_client_attrs.h"
#include "mongo/transport/asio/asio_session_impl.h"
#include "mongo/util/processinfo.h"

namespace mongo::audit {

namespace {

constexpr auto kActivityIdField = "activity_id"_sd;
constexpr auto kActorField = "actor"_sd;
constexpr auto kAnonymous = "anonymous"_sd;
constexpr auto kCategoryIdField = "category_uid"_sd;
constexpr auto kClassUidField = "class_uid"_sd;
constexpr auto kCorrelationUIDField = "correlation_uid"_sd;
constexpr auto kDataField = "data"_sd;
constexpr auto kDestinationEndpointField = "dst_endpoint"_sd;
constexpr auto kFullNameField = "full_name"_sd;
constexpr auto kGroupsField = "groups"_sd;
constexpr auto kInterfaceNameField = "interface_name"_sd;
constexpr auto kIntermediateEndpointsField = "intermediate_ips"_sd;
constexpr auto kIPField = "ip"_sd;
constexpr auto kMetadataField = "metadata"_sd;
constexpr auto kNameField = "name"_sd;
constexpr auto kOSField = "os"_sd;
constexpr auto kPortField = "port"_sd;
constexpr auto kPrivilegesField = "privileges"_sd;
constexpr auto kProcessIdField = "pid"_sd;
constexpr auto kProductField = "product"_sd;
constexpr auto kSeverityIdField = "severity_id"_sd;
constexpr auto kSourceEndpointField = "src_endpoint"_sd;
constexpr auto kTimestampField = "time"_sd;
constexpr auto kTypeField = "type"_sd;
constexpr auto kTypeIDField = "type_id"_sd;
constexpr auto kTypeUID = "type_uid"_sd;
constexpr auto kUnixField = "unix"_sd;
constexpr auto kUserField = "user"_sd;
constexpr auto kVersionField = "version"_sd;

constexpr auto kOCSFSchemaVersion = "1.0.0"_sd;
constexpr auto kRegularUserTypeInt = 1;
constexpr auto kSystemUserTypeInt = 3;
constexpr auto kTypeIdUnknown = 0;
constexpr auto kTypeIdWindows = 100;
constexpr auto kTypeIdLinux = 200;
constexpr auto kTypeIdMacOS = 300;

void serializeHostAndPortToBSONOCSF(const HostAndPort& hp, BSONObjBuilder* bob) {
    if (hp.hasPort()) {
        bob->append(kIPField, hp.host());
        bob->append(kPortField, hp.port());
    } else {
        bob->append(kInterfaceNameField, kUnixField);
        bob->append(kIPField, hp.host().empty() ? kAnonymous : StringData(hp.host()));
    }
}

void _serializeClient(BSONObjBuilder* builder, Client* client) {
    BSONObjBuilder actor(builder->subobjStart(kActorField));

    AuditOCSF::AuditEventOCSF::_buildUser(&actor, client);
}

}  // namespace

AuditOCSF::AuditEventOCSF::AuditEventOCSF(const TryLogEventParamsOCSF& tryLogParams) {
    _init(tryLogParams);
}

StringData AuditOCSF::AuditEventOCSF::getTimestampFieldName() const {
    return kTimestampField;
}

void AuditOCSF::AuditEventOCSF::_init(const TryLogEventParamsOCSF& tryLogParams) {
    AuditInterface::AuditEvent::_ts = Date_t::now();
    BSONObjBuilder builder;

    builder.append(kActivityIdField, tryLogParams.activityId);

    builder.append(kCategoryIdField, tryLogParams.ocsfEventCategory);
    builder.append(kClassUidField, tryLogParams.ocsfEventClass);
    if (!getGlobalAuditManager()->getEncryptionEnabled()) {
        builder.append(kTimestampField, getTimestamp().toMillisSinceEpoch());
    }
    builder.append(kSeverityIdField, tryLogParams.severity);

    builder.append(kTypeUID,
                   OCSFEventClass_serializer(tryLogParams.ocsfEventClass) * 100 +
                       tryLogParams.activityId);


    {
        BSONObjBuilder metadataBuilder(builder.subobjStart(kMetadataField));
        if (auto client = tryLogParams.client) {
            metadataBuilder.append(kCorrelationUIDField, client->getUUID().toString());
        }
        metadataBuilder.append(kProductField, "MongoDB Server");
        metadataBuilder.append(kVersionField, kOCSFSchemaVersion);
    }

    _serializeClient(&builder, tryLogParams.client);

    if (tryLogParams.serializer) {
        tryLogParams.serializer(&builder);
    }

    AuditInterface::AuditEvent::_obj = builder.obj<BSONObj::LargeSizeTrait>();
}

void AuditOCSF::AuditEventOCSF::_buildNetwork(Client* client, BSONObjBuilder* builder) {
    invariant(client);

    if (auto attrs = rpc::AuditClientAttrs::get(client)) {
        {
            // Serialize the dst_endpoint field
            BSONObjBuilder dstBuilder = builder->subobjStart(kDestinationEndpointField);
            serializeHostAndPortToBSONOCSF(attrs->getLocal(), &dstBuilder);
        }

        {
            // Serialize the src_endpoint field
            BSONObjBuilder srcBuilder = builder->subobjStart(kSourceEndpointField);
            serializeHostAndPortToBSONOCSF(attrs->getRemote(), &srcBuilder);
            if (auto intermediates = attrs->getProxiedEndpoints(); !intermediates.empty()) {
                BSONArrayBuilder intermediatesBuilder(
                    srcBuilder.subarrayStart(kIntermediateEndpointsField));
                for (const auto& intermediate : intermediates) {
                    BSONObjBuilder arrValue = intermediatesBuilder.subobjStart();
                    serializeHostAndPortToBSONOCSF(intermediate, &arrValue);
                }
            }
        }
    }
}

void AuditOCSF::AuditEventOCSF::_buildUser(BSONObjBuilder* builder, const UserName& userName) {
    BSONObjBuilder user(builder->subobjStart(kUserField));
    user.append(kTypeIDField, ocsf::kUserTypeIdRegularUser);
    user.append(kNameField, userName.getUnambiguousName());
}

void AuditOCSF::AuditEventOCSF::_buildUser(BSONObjBuilder* builder,
                                           const UserName& userName,
                                           RoleNameIterator roles) {
    BSONObjBuilder user(builder->subobjStart(kUserField));
    user.append(kTypeIDField, ocsf::kUserTypeIdRegularUser);
    user.append(kNameField, userName.getUnambiguousName());

    {
        BSONArrayBuilder groups(user.subarrayStart(kGroupsField));
        while (roles.more()) {
            BSONObjBuilder group(groups.subobjStart());
            group.append(kNameField, roles.next().getUnambiguousName());
        }
    }
}

void AuditOCSF::AuditEventOCSF::_buildUser(BSONObjBuilder* builder,
                                           const UserName& userName,
                                           const std::vector<RoleName>& roles) {
    _buildUser(builder, userName, makeRoleNameIteratorForContainer(roles));
}

void AuditOCSF::AuditEventOCSF::_buildUser(BSONObjBuilder* builder,
                                           BSONObj doc,
                                           const boost::optional<TenantId>& tenantId) try {
    auto userName = UserName::parseFromBSONObj(doc, tenantId);
    _buildUser(builder, userName);
} catch (...) {
    // Swallow any exception and simply exclude the user field.
}

void AuditOCSF::AuditEventOCSF::_buildUser(BSONObjBuilder* builder, Client* client) {
    // For signal handlers, there is no client so we do not have a user to
    // serialize in these cases.
    if (!client) {
        return;
    }

    if (client->isFromSystemConnection()) {
        BSONObjBuilder user(builder->subobjStart(kUserField));
        user.append(kTypeIDField, ocsf::kUserTypeIdSystemUser);
        return;
    }

    if (AuthorizationSession::exists(client)) {
        auto as = AuthorizationSession::get(client);
        auto userName = as->getImpersonatedUserName();
        RoleNameIterator roleNames;

        if (userName) {
            roleNames = as->getImpersonatedRoleNames();
        } else {
            userName = as->getAuthenticatedUserName();
            roleNames = as->getAuthenticatedRoleNames();
        }

        if (userName) {
            _buildUser(builder, *userName, roleNames);
        }
    }
}

void AuditOCSF::AuditEventOCSF::_buildProcess(BSONObjBuilder* builder) {
    int pid = static_cast<int>(ProcessId::getCurrent().asInt64());
    builder->append(kProcessIdField, pid);
}

void AuditOCSF::AuditEventOCSF::_buildDevice(BSONObjBuilder* builder) {
    builder->append(kTypeIDField, kTypeIdUnknown);

    {
        BSONObjBuilder os(builder->subobjStart(kOSField));
        os.append(kNameField, ProcessInfo::getOsName());
        os.append(kVersionField, ProcessInfo::getOsVersion());

        const auto& osType = ProcessInfo::getOsType();

        if (osType == "linux") {
            os.append(kTypeIDField, kTypeIdLinux);
        } else if (osType == "windows") {
            os.append(kTypeIDField, kTypeIdWindows);
        } else if (osType == "macos") {
            os.append(kTypeIDField, kTypeIdMacOS);
        } else {
            os.append(kTypeIDField, kTypeIdUnknown);
        }
    }
}

void AuditOCSF::AuditEventOCSF::_buildEntity(BSONObjBuilder* builder,
                                             StringData entityId,
                                             const BSONObj* data,
                                             StringData name,
                                             StringData type) {
    BSONObjBuilder entity(builder->subobjStart(entityId));
    entity.append(kNameField, name);
    entity.append(kTypeField, type);
    if (data) {
        entity.append(kDataField, *data);
    }
}

}  // namespace mongo::audit
