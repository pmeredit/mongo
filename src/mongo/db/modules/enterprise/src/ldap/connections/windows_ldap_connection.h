/**
 *    Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "../ldap_type_aliases.h"
#include "ldap/ldap_connection_options.h"
#include "ldap_connection.h"
#include "ldap_connection_reaper.h"

#include <memory>

namespace mongo {

class Status;
class StringData;

/**
 * Implementation of LDAPConnection using native Windows LDAP API.
 *
 * See Microsoft's "Lightweight Directory Access Protocol Reference".
 */
class WindowsLDAPConnection : public LDAPConnection {
public:
    WindowsLDAPConnection(LDAPConnectionOptions options,
                          std::shared_ptr<LDAPConnectionReaper> reaper);
    ~WindowsLDAPConnection();
    Status connect() final;
    Status bindAsUser(UniqueBindOptions bindOptions,
                      TickSource* tickSource,
                      SharedUserAcquisitionStats userAcquisitionStats) final;
    boost::optional<std::string> currentBoundUser() const final;
    boost::optional<const LDAPBindOptions&> bindOptions() const final;
    Status checkLiveness(TickSource* tickSource,
                         SharedUserAcquisitionStats userAcquisitionStats) final;
    StatusWith<LDAPEntityCollection> query(LDAPQuery query,
                                           TickSource* tickSource,
                                           SharedUserAcquisitionStats userAcquisitionStats) final;
    Status disconnect() final;

    LDAPSessionId getId() const override;

private:
    class WindowsLDAPConnectionPIMPL;
    std::unique_ptr<WindowsLDAPConnectionPIMPL> _pimpl;
    std::shared_ptr<LDAPConnectionReaper> _reaper;
    boost::optional<std::string> _boundUser;

    unsigned long _timeoutSeconds;
};

}  // namespace mongo
