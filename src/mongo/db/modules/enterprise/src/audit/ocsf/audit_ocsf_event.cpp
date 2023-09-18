/**
 *    Copyright (C) 2023 10gen Inc.
 */

#include "audit_ocsf.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/multitenancy.h"
#include "mongo/transport/asio/asio_session_impl.h"
#include "mongo/util/processinfo.h"

namespace mongo::audit {

namespace {

constexpr auto kActivityIdField = "activity_id"_sd;
constexpr auto kCategoryIdField = "category_uid"_sd;
constexpr auto kSeverityIdField = "severity_id"_sd;
constexpr auto kClassUidField = "class_uid"_sd;
constexpr auto kTimestampField = "time"_sd;
constexpr auto kMetadataField = "metadata"_sd;
constexpr auto kProductField = "product"_sd;
constexpr auto kVersionField = "version"_sd;
constexpr auto kUnmappedField = "unmapped"_sd;
constexpr auto kActorField = "actor"_sd;
constexpr auto kUserField = "user"_sd;
constexpr auto kUserUIDField = "uid"_sd;
constexpr auto kTypeIDField = "type_id"_sd;
constexpr auto kNameField = "name"_sd;
constexpr auto kTypeField = "type"_sd;
constexpr auto kDataField = "data"_sd;
constexpr auto kFullNameField = "full_name"_sd;
constexpr auto kGroupField = "group"_sd;
constexpr auto kPrivilegesField = "privileges"_sd;
constexpr auto kTypeUID = "type_uid"_sd;
constexpr auto kDestinationEndpointField = "dst_endpoint"_sd;
constexpr auto kSourceEndpointField = "src_endpoint"_sd;
constexpr auto kIPField = "ip"_sd;
constexpr auto kPortField = "port"_sd;
constexpr auto kInterfaceNameField = "interface_name"_sd;
constexpr auto kUnixField = "unix"_sd;
constexpr auto kAnonymous = "anonymous"_sd;
constexpr auto kProcessIdField = "pid"_sd;
constexpr auto kOSField = "os"_sd;


constexpr auto kOCSFSchemaVersion = "1.0.0"_sd;
constexpr auto kRegularUserTypeInt = 1;
constexpr auto kSystemUserTypeInt = 3;
constexpr auto kTypeIdUnknown = 0;
constexpr auto kTypeIdWindows = 100;
constexpr auto kTypeIdLinux = 200;
constexpr auto kTypeIdMacOS = 300;

template <typename Iter>
void serializeNamesToBSON(Iter names, BSONObjBuilder* builder, StringData nameType) {
    BSONArrayBuilder namesBuilder(builder->subarrayStart(nameType));
    while (names.more()) {
        const auto& name = names.next();
        name.serializeToBSON(&namesBuilder);
    }
}

void serializeSockAddrToBSONOCSF(SockAddr& sockaddr,
                                 StringData fieldName,
                                 BSONObjBuilder* builder) {
    BSONObjBuilder bob(builder->subobjStart(fieldName));

    if (sockaddr.isIP()) {
        bob.append(kIPField, sockaddr.getAddr());
        bob.append(kPortField, static_cast<int>(sockaddr.getPort()));
    } else if (sockaddr.getType() == AF_UNIX) {
        bob.append(kInterfaceNameField, kUnixField);
        if (!sockaddr.isAnonymousUNIXSocket()) {
            bob.append(kIPField, sockaddr.getAddr());
        } else {
            bob.append(kIPField, kAnonymous);
        }
    }
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
    builder.append(kTimestampField, AuditInterface::AuditEvent::_ts);
    builder.append(kSeverityIdField, tryLogParams.severity);

    builder.append(kTypeUID,
                   OCSFEventClass_serializer(tryLogParams.ocsfEventClass) * 100 +
                       tryLogParams.activityId);


    {
        BSONObjBuilder metadataBuilder(builder.subobjStart(kMetadataField));
        metadataBuilder.append(kProductField, "MongoDB Server");
        metadataBuilder.append(kVersionField, kOCSFSchemaVersion);
    }

    _buildActor(tryLogParams.client, &builder);


    if (tryLogParams.serializer) {
        tryLogParams.serializer(&builder);
    }

    AuditInterface::AuditEvent::_obj = builder.obj<BSONObj::LargeSizeTrait>();
}

void AuditOCSF::AuditEventOCSF::_buildNetwork(Client* client, BSONObjBuilder* builder) {
    invariant(client);

    if (auto session = client->session()) {
        {
            auto remote = dynamic_cast<transport::CommonAsioSession*>(session.get())->remoteAddr();
            invariant(remote.isValid());
            serializeSockAddrToBSONOCSF(remote, kSourceEndpointField, builder);
        }

        {
            auto local = dynamic_cast<transport::CommonAsioSession*>(session.get())->localAddr();
            invariant(local.isValid());
            serializeSockAddrToBSONOCSF(local, kDestinationEndpointField, builder);
        }
    }
}

void AuditOCSF::AuditEventOCSF::_buildUser(Client* client, BSONObjBuilder* builder) {
    invariant(client);

    BSONObjBuilder user(builder->subobjStart(kUserField));

    user.append(kUserUIDField, client->getUUID().toString());

    if (client->isFromSystemConnection()) {
        user.append(kTypeIDField, kSystemUserTypeInt);
    } else {
        user.append(kTypeIDField, kRegularUserTypeInt);
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
            user.append(kNameField, userName->getUnambiguousName());
        }

        {
            BSONObjBuilder group(user.subobjStart(kGroupField));
            serializeNamesToBSON(roleNames, &group, kPrivilegesField);
        }
    }
}

void AuditOCSF::AuditEventOCSF::_buildActor(Client* client, BSONObjBuilder* builder) {
    BSONObjBuilder actor(builder->subobjStart(kActorField));

    // For signal handlers, there is no client so we do not have a user to
    // serialize in these cases.
    if (!client) {
        return;
    }

    _buildUser(client, &actor);
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
