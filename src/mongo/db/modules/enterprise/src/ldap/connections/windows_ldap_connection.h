/**
*    Copyright (C) 2016 MongoDB Inc.
*/

#pragma once

#include "../ldap_type_aliases.h"
#include "ldap_connection.h"

struct ldap;
typedef struct ldap LDAP;

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
    Status WindowsLDAPConnection::_resultCodeToStatus(StringData functionName,
                                                      StringData failureHint);
    Status WindowsLDAPConnection::_resultCodeToStatus(ULONG statusCode,
                                                      StringData functionName,
                                                      StringData failureHint);

    LDAP* _session;
    unsigned long _timeoutSeconds;
};

}  // namespace mongo
