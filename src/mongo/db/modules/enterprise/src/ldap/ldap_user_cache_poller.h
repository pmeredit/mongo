/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/client.h"
#include "mongo/util/background.h"
#include "mongo/util/time_support.h"

namespace mongo {
/**
 * Job to periodically poll all $external users and either refresh or invalidate them when LDAP is
 * enabled.
 */
class LDAPUserCachePoller : public BackgroundJob {
public:
    LDAPUserCachePoller();

protected:
    std::string name() const final;
    void run() final;

private:
    std::string _name;

    /**
     * Waits for the configured refresh interval to elapse and then tries to refresh all of the
     * users in the $external database from the authorization user cache. If the refresh pass
     * succeeds, it returns the current time as the most recent successful refresh pass. Otherwise,
     * it checks whether the staleness interval has elapsed since the last successful refresh time.
     * If so, it invalidates all $external entries in the user cache immediately.
     */
    Date_t refreshExternalEntries(Client* client, Date_t lastSuccessfulRefresh);

    /**
     * Waits for the configured invalidation interval to elapse and then invalidates all of the
     * users in the $external database from the authorization user cache. This is only called if the
     * server has been configured to invalidate $external users periodically rather than refreshing
     * them by setting the ldapShouldRefreshUserCacheEntries setParameter to false.
     */
    void waitAndInvalidateExternalEntries(Client* client);

    /**
     * Helper function that invalidates users in the $external database from the user cache
     * immediately.
     */
    void invalidateExternalEntries(OperationContext* opCtx);
};

}  // namespace mongo
