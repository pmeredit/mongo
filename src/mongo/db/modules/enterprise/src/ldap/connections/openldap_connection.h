/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_connection.h"

#include <boost/optional.hpp>
#include <ldap.h>
#include <memory>

#include "mongo/util/net/sockaddr.h"

#include "ldap_connection_reaper.h"


namespace mongo {

/**
 * Implementation of LDAPConnection using libldap
 */
class OpenLDAPConnection final : public LDAPConnection {
public:
    explicit OpenLDAPConnection(LDAPConnectionOptions options,
                                std::shared_ptr<LDAPConnectionReaper> reaper);
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

    SockAddr getPeerSockAddr() const;

private:
    class OpenLDAPConnectionPIMPL;
    std::unique_ptr<OpenLDAPConnectionPIMPL> _pimpl;  // OpenLDAP's state
    std::shared_ptr<LDAPConnectionReaper> _reaper;

    struct timeval _timeout;  // Interval of time after which OpenLDAP's connections fail
    ldap_conncb _callback;    // callback that is called on connection

    boost::optional<std::string> _boundUser;
    boost::optional<LDAPBindOptions> _bindOptions;
};

}  // namespace mongo
