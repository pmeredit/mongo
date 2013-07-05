/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_event.h"

namespace mongo {
namespace audit {

    static void putAuditOperationIdBSON(const AuditOperationId& opId, BSONObjBuilder& builder) {
        builder.append("connid", opId.getConnectionId());
        builder.appendIntOrLL("opnum", static_cast<long long>(opId.getOperationNumber()));
    }

    static void putUserNameBSON(const UserName& user, BSONObjBuilder& builder) {
        builder.append("user", user.getUser());
        builder.append("userSource", user.getDB());
    }

    static void putAllUserNamesBSON(PrincipalSet::NameIterator names, BSONArrayBuilder& builder) {
        while (names.more()) {
            BSONObjBuilder nameBuilder(builder.subobjStart());
            putUserNameBSON(names.next(), nameBuilder);
            nameBuilder.done();
        }
    }

    BSONObjBuilder& AuditEvent::putBSON(BSONObjBuilder& builder) const {
        builder.appendDate("ts", getTimestamp());
        BSONObjBuilder idBuilder(builder.subobjStart("id"));
        putAuditOperationIdBSON(getOperationId(), idBuilder);
        idBuilder.done();
        BSONArrayBuilder usersBuilder(builder.subarrayStart("users"));
        putAllUserNamesBSON(getAuthenticatedUsers(), usersBuilder);
        usersBuilder.done();
        builder.append("atype", getActionType().toString());
        builder.appendIntOrLL("result", getResultCode());
        BSONObjBuilder paramBuilder(builder.subobjStart("param"));
        putParamsBSON(paramBuilder);
        paramBuilder.done();
        return builder;
    }

}  // namespace audit
}  // namespace mongo
