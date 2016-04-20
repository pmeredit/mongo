/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_connection.h"

#include <memory>

namespace mongo {

/**
 * Implementation of LDAPConnection using libldap
 */
class OpenLDAPConnection : public LDAPConnection {
public:
    explicit OpenLDAPConnection(LDAPConnectionOptions options);
    ~OpenLDAPConnection() final;
    Status connect() final;
    Status bindAsUser(const LDAPBindOptions& params) final;
    StatusWith<LDAPEntityCollection> query(LDAPQuery query) final;
    Status disconnect() final;

private:
    class OpenLDAPConnectionPIMPL;
    std::unique_ptr<OpenLDAPConnectionPIMPL> _pimpl;  // OpenLDAP's state

    struct timeval _timeout;  // Interval of time after which OpenLDAP's connections fail
};

}  // namespace mongo
