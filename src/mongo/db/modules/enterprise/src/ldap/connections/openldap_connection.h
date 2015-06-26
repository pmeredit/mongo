/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_connection.h"

struct ldap;

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
    /**
     * Convert OpenLDAP's internal errno, acquired through ldap_get_option, to a Status.
     * This should only be called if we believe that an error has occured. It will never return
     * Status::OK(). The functionName should be provided to improve the error message.
     */
    Status _resultCodeToStatus(StringData functionName, StringData failureHint);

    struct ldap* _session;  // OpenLDAP's state

    struct timeval _timeout;  // Interval of time after which OpenLDAP's connections fail
};

}  // namespace mongo
