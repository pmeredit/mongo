/**
 *  Copyright (C) 2021 MongoDB Inc.
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

LDAPHost::LDAPHost(StringData host, bool isSSL) : _isSSL{isSSL} {
    _uriPrefix = isSSL ? "ldaps://" : "ldap://";
    StatusWith<HostAndPort> hostAsHostAndPort = HostAndPort::parse(host);
    uassertStatusOK(hostAsHostAndPort);
    parse(hostAsHostAndPort.getValue());
}

LDAPHost::LDAPHost(HostAndPort host, bool isSSL) : _isSSL{isSSL} {
    _uriPrefix = isSSL ? "ldaps://" : "ldap://";
    parse(host);
}

HostAndPort LDAPHost::serializeHostAndPort() const {
    return HostAndPort(_hostName, _port);
}

std::string LDAPHost::serializeURI() const {
    return _isIpvSix ? _uriPrefix + "[" + _hostName + "]" + ":" + std::to_string(_port)
                     : _uriPrefix + _hostName + ":" + std::to_string(_port);
}

std::string LDAPHost::getName() const {
    return _hostName;
}

int LDAPHost::getPort() const {
    return _port;
}

std::string LDAPHost::getNameAndPort() const {
    return _isIpvSix ? "[" + _hostName + "]" + ":" + std::to_string(_port)
                     : _hostName + ":" + std::to_string(_port);
}

void LDAPHost::parse(const HostAndPort& host) {
    _hostName = host.host();
    _isIpvSix = ((int)std::count(_hostName.begin(), _hostName.end(), ':') > 1);
    if (host.hasPort()) {
        _port = host.port();
    } else {
        _port = _isSSL ? 636 : 389;
    }
    _isIpvFour = _isIpvSix ? false : CIDR::parse(_hostName).isOK();
}

std::string joinLdapHost(std::vector<LDAPHost> hosts, char joinChar) {
    StringBuilder s;
    for (size_t i = 0; i < hosts.size(); i++) {
        if (i != 0) {
            s << joinChar;
        }
        s << hosts[i].getName();
    }
    return s.str();
}
std::string joinLdapHostAndPort(std::vector<LDAPHost> hosts, char joinChar) {
    StringBuilder s;
    for (size_t i = 0; i < hosts.size(); i++) {
        if (i != 0) {
            s << joinChar;
        }
        s << hosts[i].getNameAndPort();
    }
    return s.str();
}
}  // namespace mongo
