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
    enum class Type {
        // TODO - add comments
        kDefault,
        kSRV,
        kSRVRaw,
    };

    LDAPHost(Type type, StringData host, bool isSSL);
    LDAPHost(Type type, HostAndPort host, bool isSSL);

    HostAndPort serializeHostAndPort() const;
    std::string serializeURI() const;
    std::string getName() const;
    std::string getNameAndPort() const;

    int getPort() const;
    bool isSSL() const {
        return _isSSL;
    }
    bool isIpvFour() const {
        return _isIpvFour;
    }
    bool isIpvSix() const {
        return _isIpvSix;
    }

    Type getType() const {
        return _type;
    }

    std::string toString() const;

private:
    void parse(const HostAndPort& host);

private:
    std::string _hostName;
    bool _isSSL{false};
    bool _isIpvFour{false};
    bool _isIpvSix{false};
    int _port{0};
    Type _type{Type::kDefault};
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
