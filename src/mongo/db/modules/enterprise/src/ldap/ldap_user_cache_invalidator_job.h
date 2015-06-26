/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <string>

#include "mongo/util/background.h"

namespace mongo {
/**
 * Job to periodically invalidate all $external users, when LDAP is enabled
 */
class LDAPUserCacheInvalidator : public BackgroundJob {
protected:
    std::string name() const final;
    void run() final;
};

}  // namespace mongo
