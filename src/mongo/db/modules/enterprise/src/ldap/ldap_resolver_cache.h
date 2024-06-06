/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
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
    LDAPResolvedHost(std::string hostName, std::string address, int port, bool isSSl)
        : _hostName(hostName), _address(address), _port(port), _isSSL(isSSl) {}
    /**
     * Methods to serialize as HostAndPort object and SockAddr object
     **/
    HostAndPort serializeHostAndPort() const;
    SockAddr serializeSockAddr() const;

    LDAPHost toLDAPHost() const;

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

private:
    std::string _hostName;
    std::string _address;
    int _port;
    bool _isSSL;
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

public:
    struct CacheKey {
        LDAPHost::Type type;
        std::string hostName;

        friend bool operator==(const CacheKey& lhs, const CacheKey& rhs);

        template <typename H>
        friend H AbslHashValue(H h, const CacheKey& c) {
            return H::combine(std::move(h), c.type, c.hostName);
        }
    };

private:
    struct CacheEntry {
        std::string name;
        // Only applicable for SRV records
        int port{-1};
        Date_t expiration;
    };

    /**
     * Method to perform networking DNS resolution, and return resolved host as LDAPResolvedHost
     **/
    CacheEntry _resolveHost(const LDAPHost& host);

    LDAPResolvedHost _getResolvedHost(const LDAPHost& host, const CacheEntry& entry);

private:
    stdx::unordered_map<CacheKey, CacheEntry> _dnsCache;
    Mutex _mutex = MONGO_MAKE_LATCH("WindowsLDAPInfoCache::_mutex");
};

inline bool operator==(const LDAPDNSResolverCache::CacheKey& lhs,
                       const LDAPDNSResolverCache::CacheKey& rhs) {
    return lhs.type == rhs.type && lhs.hostName == rhs.hostName;
}

}  // namespace mongo
