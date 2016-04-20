/**
*    Copyright (C) 2016 MongoDB Inc.
*/

#pragma once

#include "../ldap_type_aliases.h"
#include "ldap_connection.h"

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
    WindowsLDAPConnection(LDAPConnectionOptions options);
    ~WindowsLDAPConnection();
    virtual Status connect() final;
    virtual Status bindAsUser(const LDAPBindOptions& options) final;
    virtual StatusWith<LDAPEntityCollection> query(LDAPQuery query) final;
    virtual Status disconnect() final;

private:
    class WindowsLDAPConnectionPIMPL;
    std::unique_ptr<WindowsLDAPConnectionPIMPL> _pimpl;

    unsigned long _timeoutSeconds;
};

}  // namespace mongo
