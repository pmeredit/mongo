/**
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_event.h"
#include "mongo/db/matcher/matchable.h"

namespace mongo {
namespace audit {

    static void putRoleNamesBSON(RoleNameIterator roles, BSONArrayBuilder& builder) {
        while (roles.more()) {
            const RoleName& role = roles.next();
            BSONObjBuilder roleNameBuilder(builder.subobjStart());
            roleNameBuilder.append(AuthorizationManager::ROLE_NAME_FIELD_NAME, role.getRole());
            roleNameBuilder.append(AuthorizationManager::ROLE_SOURCE_FIELD_NAME, role.getDB());
        }
    }

    static void putUserNamesBSON(UserNameIterator users, BSONArrayBuilder& builder) {
        while (users.more()) {
            const UserName& user = users.next();
            BSONObjBuilder userNameBuilder(builder.subobjStart());
            userNameBuilder.append(AuthorizationManager::USER_NAME_FIELD_NAME, user.getUser());
            userNameBuilder.append(AuthorizationManager::USER_DB_FIELD_NAME, user.getDB());
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
            UserNameIterator userNames = getImpersonatedUserNames();
            RoleNameIterator roleNames = getImpersonatedRoleNames();

            if (!userNames.more()) {
                userNames = getAuthenticatedUserNames();
                roleNames = getAuthenticatedRoleNames();
            }

            BSONArrayBuilder usersBuilder(builder.subarrayStart("users"));
            putUserNamesBSON(userNames, usersBuilder);
            usersBuilder.done();
            BSONArrayBuilder rolesBuilder(builder.subarrayStart("roles"));
            putRoleNamesBSON(roleNames, rolesBuilder);
            rolesBuilder.done();
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
