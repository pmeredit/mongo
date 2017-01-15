/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "ldap_user_cache_invalidator_job.h"

#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/client.h"
#include "mongo/db/server_parameters.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

#include "ldap_options.h"

namespace mongo {
namespace {
AtomicInt32 ldapUserCacheInvalidationInterval(30);  // 30 second default
stdx::mutex invalidationIntervalMutex;
stdx::condition_variable invalidationIntervalChanged;

// TODO: Factor this and the user_cache_invalidator_job into a single
// 'exported condition parameter' class
class ExportedLDAPInvalidationIntervalParameter
    : public ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime> {
public:
    ExportedLDAPInvalidationIntervalParameter()
        : ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime>(
              ServerParameterSet::getGlobal(),
              "ldapUserCacheInvalidationInterval",
              &ldapUserCacheInvalidationInterval) {}
    Status validate(const std::int32_t& potentialNewValue) final {
        if (potentialNewValue < 1 || potentialNewValue > 86400) {
            return Status(ErrorCodes::BadValue,
                          "ldapUserCacheInvalidationIntervalSecs must be between 1 "
                          "and 86400 (24 hours)");
        }
        return Status::OK();
    }

    using ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime>::set;
    Status set(const std::int32_t& newValue) final {
        stdx::unique_lock<stdx::mutex> lock(invalidationIntervalMutex);
        Status status =
            ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime>::set(
                newValue);
        invalidationIntervalChanged.notify_all();
        return status;
    }
} exportedIntervalParam;
}  // namespace

std::string LDAPUserCacheInvalidator::name() const {
    return "LDAPUserCacheInvalidatorThread";
}

void LDAPUserCacheInvalidator::run() {
    Client::initThread("LDAPUserCacheInvalidator");
    while (!globalInShutdownDeprecated()) {
        Date_t start = Date_t::now();
        Date_t wakeupTime;
        do {
            stdx::unique_lock<stdx::mutex> lock(invalidationIntervalMutex);
            wakeupTime = start + Seconds(ldapUserCacheInvalidationInterval.load());
            invalidationIntervalChanged.wait_until(lock, wakeupTime.toSystemTimePoint());
        } while (wakeupTime > Date_t::now());
        log() << "Invalidating user cache entries of external users";
        getGlobalAuthorizationManager()->invalidateUsersFromDB("$external");
    }
}

}  // namespace mongo
