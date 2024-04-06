/**
 *  Copyright (C) 2021 MongoDB Inc.
 */


#include "mongo/platform/basic.h"

#include "ldap_user_cache_poller.h"

#include "ldap/ldap_user_cache_poller_gen.h"
#include "mongo/base/init.h"
#include "mongo/base/parse_number.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/exit.h"

#include "ldap_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {
namespace {
AtomicWord<int> ldapUserCacheInvalidationInterval(
    LDAPUserCacheInvalidationIntervalParameter::kDataDefault);  // seconds
Mutex invalidationIntervalMutex = MONGO_MAKE_LATCH();
stdx::condition_variable invalidationIntervalChanged;

AtomicWord<int> ldapUserCacheRefreshInterval(
    LDAPUserCacheRefreshIntervalParameter::kDataDefault);  // seconds
Mutex refreshIntervalMutex = MONGO_MAKE_LATCH();
stdx::condition_variable refreshIntervalChanged;

AtomicWord<int> ldapUserCacheStalenessInterval(
    LDAPUserCacheStalenessIntervalParameter::kDataDefault);  // seconds
Mutex stalenessIntervalMutex = MONGO_MAKE_LATCH();

StatusWith<int> parseSecondsParameter(StringData paramName, StringData paramValue) {
    int value;
    auto status = NumberParser{}(paramValue, &value);
    if (!status.isOK()) {
        return {ErrorCodes::BadValue,
                str::stream() << paramName << " must be a numeric value, '" << paramValue
                              << "' provided"};
    }

    if ((value < 1) || (value > 86400)) {
        return {ErrorCodes::BadValue,
                str::stream() << paramName << " must be between 1 and 86400 (24 hours)"};
    }

    return value;
}

}  // namespace

void LDAPUserCacheInvalidationIntervalParameter::append(OperationContext*,
                                                        BSONObjBuilder* b,
                                                        StringData name,
                                                        const boost::optional<TenantId>&) {
    *b << name << ldapUserCacheInvalidationInterval.load();
}

Status LDAPUserCacheInvalidationIntervalParameter::setFromString(StringData str,
                                                                 const boost::optional<TenantId>&) {
    StatusWith<int> parsedSeconds = parseSecondsParameter(name(), str);
    if (!parsedSeconds.isOK()) {
        return parsedSeconds.getStatus();
    }

    stdx::unique_lock<Latch> lock(invalidationIntervalMutex);
    ldapUserCacheInvalidationInterval.store(parsedSeconds.getValue());
    invalidationIntervalChanged.notify_all();

    return Status::OK();
}

void LDAPUserCacheRefreshIntervalParameter::append(OperationContext*,
                                                   BSONObjBuilder* b,
                                                   StringData name,
                                                   const boost::optional<TenantId>&) {
    *b << name << ldapUserCacheRefreshInterval.load();
}

Status LDAPUserCacheRefreshIntervalParameter::setFromString(StringData str,
                                                            const boost::optional<TenantId>&) {
    StatusWith<int> parsedSeconds = parseSecondsParameter(name(), str);
    if (!parsedSeconds.isOK()) {
        return parsedSeconds.getStatus();
    }

    stdx::unique_lock<Latch> lock(refreshIntervalMutex);
    ldapUserCacheRefreshInterval.store(parsedSeconds.getValue());
    refreshIntervalChanged.notify_all();

    return Status::OK();
}

void LDAPUserCacheStalenessIntervalParameter::append(OperationContext*,
                                                     BSONObjBuilder* b,
                                                     StringData name,
                                                     const boost::optional<TenantId>&) {
    *b << name << ldapUserCacheStalenessInterval.load();
}

Status LDAPUserCacheStalenessIntervalParameter::setFromString(StringData str,
                                                              const boost::optional<TenantId>&) {
    StatusWith<int> parsedSeconds = parseSecondsParameter(name(), str);
    if (!parsedSeconds.isOK()) {
        return parsedSeconds.getStatus();
    }

    stdx::unique_lock<Latch> lock(stalenessIntervalMutex);
    ldapUserCacheStalenessInterval.store(parsedSeconds.getValue());

    return Status::OK();
}

LDAPUserCachePoller::LDAPUserCachePoller() {
    if (ldapShouldRefreshUserCacheEntries) {
        _name = "LDAPUserCacheRefresher";
    } else {
        _name = "LDAPUserCacheInvalidator";
    }
}

std::string LDAPUserCachePoller::name() const {
    return _name + "Thread";
}

Date_t LDAPUserCachePoller::refreshExternalEntries(Client* client, Date_t lastSuccessfulRefresh) {
    Date_t start = Date_t::now();
    Date_t wakeupTime;
    {
        stdx::unique_lock<Latch> lock(refreshIntervalMutex);
        do {
            wakeupTime = start + Seconds(ldapUserCacheRefreshInterval.load());
            refreshIntervalChanged.wait_until(lock, wakeupTime.toSystemTimePoint());
        } while (wakeupTime > Date_t::now());
    }

    LOGV2_DEBUG(5914800, 1, "Refreshing user cache entries of external users");
    auto opCtx = client->makeOperationContext();
    Status status =
        AuthorizationManager::get(opCtx->getService())->refreshExternalUsers(opCtx.get());

    if (status.isOK()) {
        // Reset timer since last successful refresh.
        lastSuccessfulRefresh = Date_t::now();
    } else {
        // Check if the time passed since last successful refresh is greater than
        // ldapUserCacheStalenessInterval. If so, the cache will be fully invalidated of external
        // users to prevent keeping stale entries.
        Date_t latestStaleTime;
        {
            stdx::lock_guard<Latch> lg(stalenessIntervalMutex);
            latestStaleTime =
                lastSuccessfulRefresh + Seconds(ldapUserCacheStalenessInterval.load());
        }

        if (Date_t::now() > latestStaleTime) {
            LOGV2_WARNING(5914802,
                          "Staleness interval has expired since last successful user cache refresh "
                          "of LDAP users; invalidating user cache entries of external users",
                          "lastSuccessfulRefreshTime"_attr = lastSuccessfulRefresh);
            invalidateExternalEntries(opCtx.get());
        }
    }

    return lastSuccessfulRefresh;
}

void LDAPUserCachePoller::waitAndInvalidateExternalEntries(Client* client) {
    Date_t start = Date_t::now();
    Date_t wakeupTime;
    {
        stdx::unique_lock<Latch> lock(invalidationIntervalMutex);
        do {
            wakeupTime = start + Seconds(ldapUserCacheInvalidationInterval.load());
            invalidationIntervalChanged.wait_until(lock, wakeupTime.toSystemTimePoint());
        } while (wakeupTime > Date_t::now());
    }

    LOGV2_DEBUG(24036, 1, "Invalidating user cache entries of external users");
    auto opCtx = client->makeOperationContext();

    invalidateExternalEntries(opCtx.get());
}

void LDAPUserCachePoller::invalidateExternalEntries(OperationContext* opCtx) {
    try {
        AuthorizationManager::get(opCtx->getService())
            ->invalidateUsersFromDB(DatabaseName::kExternal);
    } catch (const DBException& e) {
        LOGV2_WARNING(24037, "Error invalidating user cache", "status"_attr = e.toStatus());
    }
}

void LDAPUserCachePoller::run() {
    // This client is killable. When interrupted (also when we encounter any DBException) during a
    // run, we will warn, and retry after the configured interval.
    Client::initThread(_name, getGlobalServiceContext()->getService(ClusterRole::ShardServer));

    Date_t lastSuccessfulRefresh = Date_t::now();
    auto client = Client::getCurrent();

    while (!globalInShutdownDeprecated() && globalLDAPParams->isLDAPAuthzEnabled()) {
        try {
            if (ldapShouldRefreshUserCacheEntries) {
                lastSuccessfulRefresh = refreshExternalEntries(client, lastSuccessfulRefresh);
            } else {
                waitAndInvalidateExternalEntries(client);
            }
        } catch (const DBException& ex) {
            LOGV2_WARNING(7466003,
                          "Error occurred during LDAP user cache poll",
                          "error"_attr = ex.toStatus());
        }
    }
}

}  // namespace mongo
