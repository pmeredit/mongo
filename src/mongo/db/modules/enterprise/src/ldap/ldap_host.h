/**
 *  Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {
/**
 * LDAPHost stores host name, port, and ssl information for a LDAP host connection
 */
class LDAPHost {
public:
    LDAPHost(StringData host, bool isSSL);
    LDAPHost(HostAndPort host, bool isSSL);

    HostAndPort serializeHostAndPort() const;
    std::string serializeURI() const;
    std::string getName() const;
    int getPort() const;
    bool isSSL() const {
        return _isSSL;
    }
    std::string getNameAndPort() const;

private:
    void parse(const HostAndPort& host);

    std::string _hostName;
    std::string _uriPrefix;
    bool _isSSL{false};
    bool _isIpvSix{false};
    int _port{0};
};

/**
 * joinLdapHost concatenates a vector of LDAPHost objects (using LDAPHost.getName()), with the
 * joinChar inbetween each host
 */
std::string joinLdapHost(std::vector<LDAPHost> hosts, char joinChar);
/**
 * joinLdapHostAndPort concatenates a vector of LDAPHost objects (using LDAPHost.getNameAndPort()),
 * with the joinChar inbetween each host
 */
std::string joinLdapHostAndPort(std::vector<LDAPHost> hosts, char joinChar);
}  // namespace mongo
