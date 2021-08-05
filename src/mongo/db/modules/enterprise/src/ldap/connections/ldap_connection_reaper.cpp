/**
 *  Copyright (C) 2021 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_connection_reaper.h"

#include "mongo/base/status_with.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/functional.h"
#include "mongo/util/out_of_line_executor.h"
#include "mongo/util/thread_safety_context.h"
#include "mongo/util/tick_source.h"

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

void LDAPConnectionReaper::scheduleReapOrDisconnectInline(reapFunc reaper) {
    // If the LDAP connection is being reaped before it is safe to spawn multiple threads, the reap
    // should occur inline rather than being scheduled in the executor. This scenario may occur if
    // the LDAP smoke test fails upon server startup, before multithreading is enabled.
    if (ThreadSafetyContext::getThreadSafetyContext()->isSingleThreaded()) {
        reaper();
    } else {
        _executor->schedule([r = std::move(reaper)](Status schedStatus) {
            if (schedStatus.isOK()) {
                r();
            }
            // Else if we are shutdown or running inline, leak the LDAP connection since the server
            // is shutting down
        });
    }
}

void LDAPConnectionReaper::reap(LDAP* ldap, TickSource* tickSource) {
    scheduleReapOrDisconnectInline(
        [ldap, &tickSource] { disconnectLDAPConnection(ldap, tickSource); });
}

}  // namespace mongo
