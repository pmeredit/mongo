/**
 *  Copyright (C) 2021 MongoDB Inc.
 */

#pragma once
#include <string>
#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/sockaddr.h"
#include "mongo/util/time_support.h"

#include "ldap_host.h"

namespace mongo {
/**
 * LDAP host that has been DNS resolved
 **/
class LDAPResolvedHost {
public:
    LDAPResolvedHost(
        std::string hostName, std::string address, int port, bool isSSl, Date_t expiration)
        : _hostName(hostName),
          _address(address),
          _port(port),
          _isSSL(isSSl),
          _expiration(expiration) {}
    /**
     * Methods to serialize as HostAndPort object and SockAddr object
     **/
    HostAndPort serializeHostAndPort() const;
    SockAddr serializeSockAddr() const;

    /**
     * Getters for memebers of LDAPResolvedHost
     **/
    std::string getAddress() const {
        return _address;
    }
    std::string getHostName() const {
        return _hostName;
    }
    int getPort() const {
        return _port;
    }
    bool isExpired() const {
        return _expiration <= Date_t::now();
    }

private:
    std::string _hostName;
    std::string _address;
    int _port;
    bool _isSSL;
    Date_t _expiration;
};

/**
 * Cache which stores DNS resolved LDAP hosts.
 **/
class LDAPDNSResolverCache {
public:
    LDAPDNSResolverCache() = default;

    /**
     * Does a DNS resolution to resolve LDAPHost to one or more LDAPResolvedHost
     * Caches DNS responses until TTL. If entry is expired, resends lookup first
     **/
    StatusWith<LDAPResolvedHost> resolve(const LDAPHost& host);

private:
    /**
     * Method to perform networking DNS resolution, and return resolved host as LDAPResolvedHost
     **/
    LDAPResolvedHost _resolveHost(const LDAPHost& host);
    stdx::unordered_map<std::string, LDAPResolvedHost> _dnsCache;
    Mutex _mutex = MONGO_MAKE_LATCH("WindowsLDAPInfoCache::_mutex");
};
}  // namespace mongo
