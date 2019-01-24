/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "ldap_user_cache_invalidator_job.h"

#include "ldap/ldap_user_cache_invalidator_job_gen.h"
#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

#include "ldap_options.h"

namespace mongo {
namespace {
AtomicWord<int> ldapUserCacheInvalidationInterval(
    LDAPUserCacheInvalidationIntervalParameter::kDataDefault);  // seconds
stdx::mutex invalidationIntervalMutex;
stdx::condition_variable invalidationIntervalChanged;
}  // namespace

void LDAPUserCacheInvalidationIntervalParameter::append(OperationContext*,
                                                        BSONObjBuilder& b,
                                                        const std::string& name) {
    b << name << ldapUserCacheInvalidationInterval.load();
}

Status LDAPUserCacheInvalidationIntervalParameter::setFromString(const std::string& str) {
    int value;
    auto status = parseNumberFromString(str, &value);
    if (!status.isOK()) {
        return {ErrorCodes::BadValue,
                str::stream() << name() << " must be a numeric value, '" << str << "' provided"};
    }

    if ((value < 1) || (value > 86400)) {
        return {ErrorCodes::BadValue,
                str::stream() << name() << " must be between 1 and 86400 (24 hours)"};
    }

    stdx::unique_lock<stdx::mutex> lock(invalidationIntervalMutex);
    ldapUserCacheInvalidationInterval.store(value);
    invalidationIntervalChanged.notify_all();

    return Status::OK();
}

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

        LOG(1) << "Invalidating user cache entries of external users";

        auto opCtx = Client::getCurrent()->makeOperationContext();
        try {
            AuthorizationManager::get(opCtx->getServiceContext())
                ->invalidateUsersFromDB(opCtx.get(), "$external");
        } catch (const DBException& e) {
            warning() << "Error invalidating user cache: " << e.toStatus();
        }
    }
}

}  // namespace mongo
