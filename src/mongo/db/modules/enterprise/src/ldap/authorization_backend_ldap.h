/**
 *  Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/auth/authorization_backend_local.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/util/assert_util.h"

#include "ldap_user_cache_poller.h"

namespace mongo::auth {

class AuthorizationBackendLDAP : public AuthorizationBackendLocal {
public:
    Status initialize(OperationContext* opCtx) final;

protected:
    Status getUserDescription(OperationContext* opCtx,
                              const UserRequest& user,
                              BSONObj* result,
                              const SharedUserAcquisitionStats& userAcquisitionStats) final;

    StatusWith<User> getUserObject(OperationContext* opCtx,
                                   const UserRequest& userReq,
                                   const SharedUserAcquisitionStats& userAcquisitionStats) final;


private:
    /**
     * Set to 0 if the invalidator has not been started, 1 if it has been started
     */
    AtomicWord<unsigned> _hasInitializedInvalidation;

    /**
     * Long running job to periodically refresh or invalidate all LDAP authorized users on $external
     */
    LDAPUserCachePoller _poller;
};
}  // namespace mongo::auth
