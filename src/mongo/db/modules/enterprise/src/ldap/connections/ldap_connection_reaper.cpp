/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "ldap_connection_reaper.h"

#include "mongo/base/status_with.h"
#include "mongo/logv2/log.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/functional.h"
#include "mongo/util/out_of_line_executor.h"
#include "mongo/util/thread_safety_context.h"
#include "mongo/util/tick_source.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(ldapConnectionReaperDestroyed);

static inline std::shared_ptr<ThreadPoolInterface> _makeThreadPool() {
    ThreadPool::Options opts;
    opts.poolName = "LDAPConnReaper";
    opts.maxThreads = ThreadPool::Options::kUnlimited;
    opts.maxIdleThreadAge = Seconds{5};

    return std::make_shared<ThreadPool>(opts);
}

}  // namespace

LDAPConnectionReaper::LDAPConnectionReaper() : _executor(_makeThreadPool()) {}

LDAPConnectionReaper::~LDAPConnectionReaper() {
    _executor.reset();
    if (MONGO_unlikely(ldapConnectionReaperDestroyed.shouldFail())) {
        LOGV2(6152901, "LDAP Connection reaper is destroyed");
    }
}

void LDAPConnectionReaper::scheduleReapOrDisconnectInline(reapFunc reaper) {
    // If the LDAP connection is being reaped before it is safe to spawn multiple threads, the reap
    // should occur inline rather than being scheduled in the executor. This scenario may occur if
    // the LDAP smoke test fails upon server startup, before multithreading is enabled.
    if (ThreadSafetyContext::getThreadSafetyContext()->isSingleThreaded()) {
        LOGV2_DEBUG(5945600, 3, "Reaping connection inline");
        reaper();
    } else {
        std::call_once(_initExecutor, [this]() { _executor->startup(); });
        _executor->schedule([r = std::move(reaper)](Status schedStatus) {
            if (schedStatus.isOK()) {
                LOGV2_DEBUG(5945601, 3, "Reaping connection in separate thread");
                r();
            }
            // Else if we are shutdown or running inline, leak the LDAP connection since the server
            // is shutting down
            LOGV2_DEBUG(7997800, 3, "Leaking LDAP connection since server is in shutdown");
        });
    }
}

void LDAPConnectionReaper::reap(LDAP* ldap) {
    scheduleReapOrDisconnectInline([ldap] {
        disconnectLDAPConnection(ldap);
        LOGV2_DEBUG(5945602, 2, "LDAP connection closed");
    });
}

}  // namespace mongo
