/**
 *  Copyright (C) 2021 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_connection_reaper.h"

#include "mongo/base/status_with.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/functional.h"
#include "mongo/util/out_of_line_executor.h"

namespace mongo {
namespace {

static inline ThreadPool::Options _makeThreadPoolOptions() {
    ThreadPool::Options opts;
    opts.poolName = "LDAPConnReaper";
    opts.maxThreads = ThreadPool::Options::kUnlimited;
    opts.maxIdleThreadAge = Seconds{5};

    return opts;
}
}  // namespace

LDAPConnectionReaper::LDAPConnectionReaper()
    : _executor(std::make_shared<ThreadPool>(_makeThreadPoolOptions())) {}

void LDAPConnectionReaper::scheduleReap(reapFunc reaper) {
    _executor->schedule([r = std::move(reaper)](Status schedStatus) {
        if (schedStatus.isOK()) {
            r();
        }
        // Else if we are shutdown or running inline, leak the LDAP connection since the server is
        // shutting down
    });
}

void LDAPConnectionReaper::schedule(LDAP* ldap) {
    scheduleReap([ldap] { disconnectLDAPConnection(ldap); });
}

}  // namespace mongo
