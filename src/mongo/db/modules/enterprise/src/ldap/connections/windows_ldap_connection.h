/**
 *    Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "../ldap_type_aliases.h"
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
                          std::shared_ptr<LDAPConnectionReaper> reaper,
                          TickSource* tickSource,
                          UserAcquisitionStats* userAcquisitionStats);
    ~WindowsLDAPConnection();
    Status connect() final;
    Status bindAsUser(const LDAPBindOptions& options,
                      TickSource* tickSource,
                      UserAcquisitionStats* userAcquisitionStats) final;
    boost::optional<std::string> currentBoundUser() const final;
    Status checkLiveness(TickSource* tickSource, UserAcquisitionStats* userAcquisitionStats) final;
    StatusWith<LDAPEntityCollection> query(LDAPQuery query,
                                           TickSource* tickSource,
                                           UserAcquisitionStats* userAcquisitionStats) final;
    Status disconnect(TickSource* tickSource, UserAcquisitionStats* userAcquisitionStats) final;

private:
    class WindowsLDAPConnectionPIMPL;
    std::unique_ptr<WindowsLDAPConnectionPIMPL> _pimpl;
    std::shared_ptr<LDAPConnectionReaper> _reaper;
    boost::optional<std::string> _boundUser;

    unsigned long _timeoutSeconds;

    // Used to track LDAP operations in CurOp
    TickSource* _tickSource;
    UserAcquisitionStats* _userAcquisitionStats;
};

}  // namespace mongo
