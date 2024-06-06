/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "ldap_host.h"

#include <cstddef>
#include <iostream>
#include <ostream>
#include <string>
#include <vector>


#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/net/cidr.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/ssl_manager.h"

namespace mongo {
namespace {

constexpr auto ldapUriPrefix = "ldap://"_sd;
constexpr auto ldapSUriPrefix = "ldaps://"_sd;

StringData getPrefix(bool isSSL) {
    return isSSL ? ldapSUriPrefix : ldapUriPrefix;
}

}  // namespace

LDAPHost::LDAPHost(Type type, StringData host, bool isSSL) : _isSSL{isSSL}, _type(type) {
    StatusWith<HostAndPort> hostAsHostAndPort = HostAndPort::parse(host);
    uassertStatusOK(hostAsHostAndPort);
    parse(hostAsHostAndPort.getValue());
}

LDAPHost::LDAPHost(Type type, HostAndPort host, bool isSSL) : _isSSL{isSSL}, _type(type) {
    parse(host);
}

HostAndPort LDAPHost::serializeHostAndPort() const {
    return HostAndPort(_hostName, _port);
}

std::string LDAPHost::serializeURI() const {
    return _isIpvSix
        ? str::stream() << getPrefix(_isSSL) << "[" << _hostName << "]:" << std::to_string(_port)
        : str::stream() << getPrefix(_isSSL) << _hostName << ":" << std::to_string(_port);
}

std::string LDAPHost::getName() const {
    return _hostName;
}

int LDAPHost::getPort() const {
    return _port;
}

std::string LDAPHost::getNameAndPort() const {
    return _isIpvSix ? str::stream() << "[" << _hostName << "]:" << std::to_string(_port)
                     : str::stream() << _hostName << ":" << std::to_string(_port);
}

void LDAPHost::parse(const HostAndPort& host) {
    _hostName = host.host();
    _isIpvSix = (static_cast<int>(std::count(_hostName.begin(), _hostName.end(), ':') > 1));
    if (host.hasPort()) {
        _port = host.port();
    } else {
        _port = _isSSL ? 636 : 389;
    }
    _isIpvFour = _isIpvSix ? false : CIDR::parse(_hostName).isOK();

    _isLocalHost = [this]() {
        if (_hostName == "localhost") {
            return true;
        }

        auto swHostNameCIDR = CIDR::parse(_hostName);
        if (!swHostNameCIDR.isOK()) {
            return false;
        }

        if (_isIpvFour) {
            return CIDR("127.0.0.0/8").contains(swHostNameCIDR.getValue());
        }

        if (_isIpvSix) {
            return CIDR("::1/128").contains(swHostNameCIDR.getValue());
        }

        return false;
    }();
}

std::string LDAPHost::toString() const {
    return str::stream() << "(" << static_cast<int>(_type) << ", " << _hostName << ", " << _port
                         << ")";
}

std::string joinLdapHost(std::vector<LDAPHost> hosts,
                         char joinChar,
                         std::function<std::string(const LDAPHost&)> func) {
    StringBuilder s;
    for (size_t i = 0; i < hosts.size(); i++) {
        if (i != 0) {
            s << joinChar;
        }
        s << func(hosts[i]);
    }
    return s.str();
}

std::string joinLdapHost(std::vector<LDAPHost> hosts, char joinChar) {
    return joinLdapHost(hosts, joinChar, [](const LDAPHost& host) { return host.getName(); });
}

std::string joinLdapHostAndPort(std::vector<LDAPHost> hosts, char joinChar) {
    return joinLdapHost(
        hosts, joinChar, [](const LDAPHost& host) { return host.getNameAndPort(); });
}

std::string joinLdapHostAndPortForLocalhost(std::vector<LDAPHost> hosts, char joinChar) {
    return joinLdapHost(hosts, joinChar, [](const LDAPHost& host) {
        if (host.isLocalHost()) {
            return host.getNameAndPort();
        }

        return host.getName();
    });
}

}  // namespace mongo
