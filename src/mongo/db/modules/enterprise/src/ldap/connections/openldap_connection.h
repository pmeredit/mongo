/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_connection.h"

#include <boost/optional.hpp>
#include <ldap.h>
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
    Status checkLiveness() final;
    Status disconnect() final;
    boost::optional<std::string> currentBoundUser() const final;
    static bool isThreadSafe();

    const boost::optional<LDAPBindOptions>& bindOptions() const {
        return _bindOptions;
    }

private:
    class OpenLDAPConnectionPIMPL;
    std::unique_ptr<OpenLDAPConnectionPIMPL> _pimpl;  // OpenLDAP's state

    struct timeval _timeout;  // Interval of time after which OpenLDAP's connections fail
    ldap_conncb _callback;    // callback that is called on connection

    boost::optional<std::string> _boundUser;
    boost::optional<LDAPBindOptions> _bindOptions;

    /**
     * Locking OpenLDAPGlobalMutex locks a global mutex if setNeedsGlobalLock was called. Otherwise,
     * it is a no-op. This is intended to synchronize access to libldap, under known thread-unsafe
     * conditions.
     */
    class OpenLDAPGlobalMutex {
    public:
        void lock();
        void unlock();
    };

    OpenLDAPGlobalMutex _conditionalMutex;
};

}  // namespace mongo
