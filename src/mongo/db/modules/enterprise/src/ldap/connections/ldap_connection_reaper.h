/**
 *  Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#ifdef _WIN32
#include <winldap.h>
#else
#include <ldap.h>
#endif

#include "mongo/util/out_of_line_executor.h"

namespace mongo {

/**
 * Reap connections on a background thread independent of the connection pool.
 */
class LDAPConnectionReaper {
public:
    LDAPConnectionReaper();

    /**
     * Schedule the connection reaper to disconnect/unbind a LDAP session on a background thread if
     * multithreading is safe. Otherwise, it will disconnect inline.
     */
    void reap(LDAP* ldap);

private:
    using reapFunc = unique_function<void(void)>;

    void scheduleReapOrDisconnectInline(reapFunc reaper);

private:
    std::shared_ptr<OutOfLineExecutor> _executor;
};

/**
 * Per LDAP API that disconnects/unbinds a LDAP session.
 */
void disconnectLDAPConnection(LDAP* ldap);

}  // namespace mongo
