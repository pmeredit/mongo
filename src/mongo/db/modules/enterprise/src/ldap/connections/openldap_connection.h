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

    /**
     * LDAPRebindCallbackParameters is used in the OpenLDAPConnection::bindAsUser() function to pass
     * the tick source and user acquisition stats as parameters to openLDAPRebindFunction, which
     * gets called when binding to referred servers.
     */
    struct LDAPRebindCallbackParameters {
        TickSource* tickSource;
        UserAcquisitionStats* referralUserAcquisitionStats;

        LDAPRebindCallbackParameters(TickSource* source, UserAcquisitionStats* userAcquisitionStats)
            : tickSource(source), referralUserAcquisitionStats(userAcquisitionStats) {}
    };

    OpenLDAPConnection() = delete;
    explicit OpenLDAPConnection(LDAPConnectionOptions options,
                                std::shared_ptr<LDAPConnectionReaper> reaper);
    ~OpenLDAPConnection() final;
    Status connect() final;
    Status bindAsUser(UniqueBindOptions bindOptions,
                      TickSource* tickSource,
                      UserAcquisitionStats* userAcquisitionStats) final;
    StatusWith<LDAPEntityCollection> query(LDAPQuery query,
                                           TickSource* tickSource,
                                           UserAcquisitionStats* userAcquisitionStats) final;
    Status checkLiveness(TickSource* tickSource, UserAcquisitionStats* userAcquisitionStats) final;
    Status disconnect() final;
    boost::optional<std::string> currentBoundUser() const final;

    static void initTraits();
    static const ProviderTraits& getTraits() {
        return _traits;
    }

    boost::optional<const LDAPBindOptions&> bindOptions() const {
        if (_bindOptions) {
            return *_bindOptions;
        }

        return boost::none;
    }

    SockAddr getPeerSockAddr() const;

    const boost::optional<LDAPRebindCallbackParameters>& getRebindCallbackParameters() {
        return _rebindCallbackParameters;
    }

private:
    class OpenLDAPConnectionPIMPL;
    std::unique_ptr<OpenLDAPConnectionPIMPL> _pimpl;  // OpenLDAP's state
    std::shared_ptr<LDAPConnectionReaper> _reaper;

    struct timeval _timeout;  // Interval of time after which OpenLDAP's connections fail
    int _timeLimitInt;        // Storing the _timeout's seconds as an int rather than a long
    ldap_conncb _callback;    // callback that is called on connection

    boost::optional<std::string> _boundUser;
    static ProviderTraits _traits;

    // Used to track rebinds and referral counts in CurOp.
    boost::optional<LDAPRebindCallbackParameters> _rebindCallbackParameters;
};
}  // namespace mongo
