/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "mongo/platform/basic.h"

#include "audit_event.h"

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"

namespace mongo {
namespace audit {

namespace {
template <typename Iter>
void serializeNamesToBSON(Iter names, BSONObjBuilder* builder, StringData nameType) {
    BSONArrayBuilder namesBuilder(builder->subarrayStart(nameType));
    while (names.more()) {
        const auto& name = names.next();
        name.serializeToBSON(&namesBuilder);
    }
}

constexpr auto kATypeField = "atype"_sd;
constexpr auto kTimestampField = "ts"_sd;
constexpr auto kLocalEndpointField = "local"_sd;
constexpr auto kRemoteEndpointField = "remote"_sd;
constexpr auto kUsersField = "users"_sd;
constexpr auto kRolesField = "roles"_sd;
constexpr auto kParamField = "param"_sd;
constexpr auto kResultField = "result"_sd;
}  // namespace

AuditEvent::AuditEvent(Client* client,
                       AuditEventType aType,
                       Serializer serializer,
                       ErrorCodes::Error result) {
    BSONObjBuilder builder;

    builder.append(kATypeField, toStringData(aType));
    builder.append(kTimestampField, Date_t::now());
    if (client) {
        serializeClient(client, &builder);
    }

    BSONObjBuilder paramBuilder(builder.subobjStart(kParamField));
    if (serializer) {
        serializer(&paramBuilder);
    }
    paramBuilder.doneFast();

    builder.append(kResultField, result);

    _obj = builder.obj();
}

void AuditEvent::serializeClient(Client* client, BSONObjBuilder* builder) {
    invariant(client);

    auto session = client->session();
    if (session) {
        auto local = session->localAddr();
        auto remote = session->remoteAddr();
        invariant(local.isValid() && remote.isValid());
        // local: {ip: '127.0.0.1', port: 27017} or {unix: '/var/run/mongodb.sock'}
        local.serializeToBSON(kLocalEndpointField, builder);
        // remote: {ip: '::1', port: 12345} or {unix: '/var/run/mongodb.sock'}
        remote.serializeToBSON(kRemoteEndpointField, builder);
    }

    if (AuthorizationSession::exists(client)) {
        auto as = AuthorizationSession::get(client);
        UserNameIterator userNames = as->getImpersonatedUserNames();
        RoleNameIterator roleNames;

        if (userNames.more()) {
            roleNames = as->getImpersonatedRoleNames();
        } else {
            userNames = as->getAuthenticatedUserNames();
            roleNames = as->getAuthenticatedRoleNames();
        }

        // users: [{db: dbnane, user: username}, ...]
        serializeNamesToBSON(userNames, builder, kUsersField);
        // roles: [{db: dbname, role: rolename}, ...]
        serializeNamesToBSON(roleNames, builder, kRolesField);
    }
}

ElementIterator* AuditEvent::allocateIterator(const ElementPath* path) const {
    if (_iteratorUsed) {
        return new BSONElementIterator(path, _obj);
    }

    _iteratorUsed = true;
    _iterator.reset(path, _obj);
    return &_iterator;
}

void AuditEvent::releaseIterator(ElementIterator* iterator) const {
    if (iterator == &_iterator) {
        _iteratorUsed = false;
    } else {
        delete iterator;
    }
}

}  // namespace audit
}  // namespace mongo
