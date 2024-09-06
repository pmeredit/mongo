/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#ifdef _WIN32
#include <winldap.h>
#else
#include <ldap.h>
#endif

#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/util/concurrency/thread_pool_interface.h"
#include "mongo/util/tick_source.h"

#include "ldap_session_id.h"

namespace mongo {

/**
 * Reap connections on a background thread independent of the connection pool.
 */
class LDAPConnectionReaper {
public:
    LDAPConnectionReaper();

    ~LDAPConnectionReaper();

    /**
     * Schedule the connection reaper to disconnect/unbind a LDAP session on a background thread if
     * multithreading is safe. Otherwise, it will disconnect inline.
     */
    void reap(LDAP* ldap, LDAPSessionId ldapSessionId);

private:
    using reapFunc = unique_function<void(void)>;

    void scheduleReapOrDisconnectInline(LDAPSessionId ldapSessionId, reapFunc reaper);

private:
    std::once_flag _initExecutor;
    std::shared_ptr<ThreadPoolInterface> _executor;
};

/**
 * Per LDAP API that disconnects/unbinds a LDAP session.
 */
void disconnectLDAPConnection(LDAP* ldap, LDAPSessionId ldapSessionId);

}  // namespace mongo
