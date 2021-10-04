/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_connection.h"

#include <boost/optional.hpp>
#include <ldap.h>
#include <memory>

#include "mongo/util/net/sockaddr.h"
#include "mongo/util/tick_source.h"

#include "ldap_connection_reaper.h"


namespace mongo {

/**
 * Implementation of LDAPConnection using libldap
 */
class OpenLDAPConnection final : public LDAPConnection {
public:
    struct ProviderTraits {
        std::string tlsPackage;
        bool mozNSSCompat;
        bool threadSafe;
        bool slowLocking;
        bool poolingSafe;
    };

    OpenLDAPConnection() = delete;
    explicit OpenLDAPConnection(LDAPConnectionOptions options,
                                std::shared_ptr<LDAPConnectionReaper> reaper,
                                TickSource* tickSource,
                                UserAcquisitionStats* userAcquisitionStats);
    ~OpenLDAPConnection() final;
    Status connect() final;
    Status bindAsUser(const LDAPBindOptions& options,
                      TickSource* tickSource,
                      UserAcquisitionStats* userAcquisitionStats) final;
    StatusWith<LDAPEntityCollection> query(LDAPQuery query,
                                           TickSource* tickSource,
                                           UserAcquisitionStats* userAcquisitionStats) final;
    Status checkLiveness(TickSource* tickSource, UserAcquisitionStats* userAcquisitionStats) final;
    Status disconnect(TickSource* tickSource, UserAcquisitionStats* userAcquisitionStats) final;
    boost::optional<std::string> currentBoundUser() const final;

    static void initTraits();
    static const ProviderTraits& getTraits() {
        return _traits;
    }

    const boost::optional<LDAPBindOptions>& bindOptions() const {
        return _bindOptions;
    }

    SockAddr getPeerSockAddr() const;

    TickSource* getTickSource() {
        return _tickSource;
    }

    UserAcquisitionStats* getUserAcquisitionStats() {
        return _userAcquisitionStats;
    }

private:
    class OpenLDAPConnectionPIMPL;
    std::unique_ptr<OpenLDAPConnectionPIMPL> _pimpl;  // OpenLDAP's state
    std::shared_ptr<LDAPConnectionReaper> _reaper;

    struct timeval _timeout;  // Interval of time after which OpenLDAP's connections fail
    ldap_conncb _callback;    // callback that is called on connection

    boost::optional<std::string> _boundUser;
    static ProviderTraits _traits;

    // Used to track LDAP operations in CurOp
    TickSource* _tickSource;
    UserAcquisitionStats* _userAcquisitionStats;
};
}  // namespace mongo
