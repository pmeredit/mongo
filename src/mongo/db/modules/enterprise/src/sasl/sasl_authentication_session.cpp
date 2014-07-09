/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "sasl_authentication_session.h"

#include <boost/range/size.hpp>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/commands.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo_gssapi.h"
#include "sasl_options.h"

namespace mongo {
    SaslAuthenticationSession::SaslSessionFactoryFn SaslAuthenticationSession::create = NULL;

    // Mechanism name constants.
    const char SaslAuthenticationSession::mechanismMONGODBCR[] = "MONGODB-CR";
    const char SaslAuthenticationSession::mechanismMONGODBX509[] = "MONGODB-X509";
    const char SaslAuthenticationSession::mechanismCRAMMD5[] = "CRAM-MD5";
    const char SaslAuthenticationSession::mechanismDIGESTMD5[] = "DIGEST-MD5";
    const char SaslAuthenticationSession::mechanismSCRAMSHA1[] = "SCRAM-SHA-1";
    const char SaslAuthenticationSession::mechanismGSSAPI[] = "GSSAPI";
    const char SaslAuthenticationSession::mechanismPLAIN[] = "PLAIN";
    
    /**
     * Standard method in mongodb for determining if "authenticatedUser" may act as "requestedUser."
     *
     * The standard rule in MongoDB is simple.  The authenticated user name must be the same as the
     * requested user name.
     */
    bool isAuthorizedCommon(SaslAuthenticationSession* session,
                            const StringData& requestedUser,
                            const StringData& authenticatedUser) {

        return requestedUser == authenticatedUser;
    }

    SaslAuthenticationSession::SaslAuthenticationSession(AuthorizationSession* authzSession) :
        AuthenticationSession(AuthenticationSession::SESSION_TYPE_SASL),
        _authzSession(authzSession),
        _saslStep(0),
        _mechInfo(NULL),
        _conversationId(0),
        _autoAuthorize(false),
        _done(false) {
    }

    SaslAuthenticationSession::~SaslAuthenticationSession() {};

    StringData SaslAuthenticationSession::getAuthenticationDatabase() const {
        if (Command::testCommandsEnabled &&
                _authenticationDatabase == "admin" &&
                getPrincipalId() == internalSecurity.user->getName().getUser()) {
            // Allows authenticating as the internal user against the admin database.  This is to
            // support the auth passthrough test framework on mongos (since you can't use the local
            // database on a mongos, so you can't auth as the internal user without this).
            return internalSecurity.user->getName().getDB();
        } else {
            return _authenticationDatabase;
        }
    }

}  // namespace mongo
