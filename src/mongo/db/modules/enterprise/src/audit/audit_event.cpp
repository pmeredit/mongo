/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_event.h"
#include "mongo/db/matcher/matchable.h"

namespace mongo {
namespace audit {

    static void putUserNameBSON(const UserName& user, BSONObjBuilder& builder) {
        builder.append(AuthorizationManager::USER_NAME_FIELD_NAME, user.getUser());
        builder.append(AuthorizationManager::USER_DB_FIELD_NAME, user.getDB());
    }

    static void putAllUserNamesBSON(UserNameIterator names, BSONArrayBuilder& builder) {
        while (names.more()) {
            BSONObjBuilder nameBuilder(builder.subobjStart());
            putUserNameBSON(names.next(), nameBuilder);
            nameBuilder.done();
        }
    }

    void AuditEvent::generateBSON() const {
        BSONObjBuilder builder;
        builder.append("atype", getActionType().toString());
        builder.appendDate("ts", getTimestamp());
        {
            BSONObjBuilder localIpBuilder(builder.subobjStart("local"));
            builder.append("ip", getLocalAddr().getAddr());
            builder.append("port", getLocalAddr().getPort());
        }
        {
            BSONObjBuilder remoteIpBuilder(builder.subobjStart("remote"));
            builder.append("ip", getRemoteAddr().getAddr());
            builder.append("port", getRemoteAddr().getPort());
        }
        {
            BSONArrayBuilder usersBuilder(builder.subarrayStart("users"));
            UserNameIterator uni = getImpersonatedUsers();
            if (uni.more()) {
                putAllUserNamesBSON(uni, usersBuilder);
            }
            else {
                putAllUserNamesBSON(getAuthenticatedUsers(), usersBuilder);
            }
        }
        {
            BSONObjBuilder paramBuilder(builder.subobjStart("param"));
            putParamsBSON(paramBuilder);
        }
        builder.appendIntOrLL("result", getResultCode());
        _obj = builder.obj();
        _bsonGenerated = true;
    }

    BSONObj AuditEvent::toBSON() const {
        if (!_bsonGenerated) {
            generateBSON();
        }
        return _obj;
    }

    ElementIterator* AuditEvent::allocateIterator( const ElementPath* path ) const {
        if (!_bsonGenerated) {
            generateBSON();
        }
        if ( _iteratorUsed )
            return new BSONElementIterator( path, _obj );
        _iteratorUsed = true;
        _iterator.reset( path, _obj );
        return &_iterator;
    }
    
    void AuditEvent::releaseIterator( ElementIterator* iterator ) const {
        if ( iterator == &_iterator ) {
            _iteratorUsed = false;
        }
        else {
            delete iterator;
        }
    }

}  // namespace audit
}  // namespace mongo
