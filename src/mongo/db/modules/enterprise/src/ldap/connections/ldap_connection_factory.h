/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>

#include "mongo/executor/connection_pool.h"

namespace mongo {

template <class T>
class StatusWith;

class LDAPConnection;
struct LDAPConnectionOptions;

/**
 * Interface for factories which produce LDAPConnection objects.
 */
class LDAPConnectionFactory {
public:
    virtual ~LDAPConnectionFactory() = default;

    explicit LDAPConnectionFactory(Milliseconds poolSetupTimeout);

    /**
     * Factory function to produce LDAP client objects
     * @param options Describes the connection to create
     *
     * @return Objects of type OpenLDAPConnection, or error
     */
    StatusWith<std::unique_ptr<LDAPConnection>> create(const LDAPConnectionOptions& options);

private:
    std::shared_ptr<executor::ConnectionPool> _pool;
};

}  // namespace mongo
