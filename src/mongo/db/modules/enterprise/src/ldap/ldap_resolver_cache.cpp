/**
 *  Copyright (C) 2021 MongoDB Inc.
 */
#include "mongo/platform/basic.h"

#include "ldap_resolver_cache.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "mongo/util/dns_query.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"


namespace mongo {
StatusWith<LDAPResolvedHost> LDAPDNSResolverCache::resolve(const LDAPHost& host) {
    if (host.isIpvSix() || host.isIpvFour()) {
        LDAPResolvedHost currHost = LDAPResolvedHost(
            host.getName(), host.getName(), host.getPort(), host.isSSL(), Date_t::now());
        return currHost;
    }

    {
        stdx::lock_guard<Latch> lk(_mutex);
        auto cachedHost = _dnsCache.find(host.getNameAndPort());
        if (cachedHost != _dnsCache.end() && !cachedHost->second.isExpired()) {
            return cachedHost->second;
        }
    }

    try {
        // Drop lock while going to the network
        LDAPResolvedHost resolvedHosts = _resolveHost(host);
        {
            stdx::lock_guard<Latch> lk(_mutex);
            _dnsCache.insert({host.getNameAndPort(), resolvedHosts});
            return resolvedHosts;
        }
    }

    catch (ExceptionFor<ErrorCodes::DNSHostNotFound>&) {
        std::string errorMsg("LDAP Host: " + host.getName() + " was NOT successfully resolved.");
        return Status(ErrorCodes::DNSHostNotFound, errorMsg);
    }
}

LDAPResolvedHost LDAPDNSResolverCache::_resolveHost(const LDAPHost& host) {
    std::vector<std::pair<std::string, Seconds>> dnsRecords = dns::lookupARecords(host.getName());
    LDAPResolvedHost currResolvedHost = LDAPResolvedHost(host.getName(),
                                                         dnsRecords.front().first,
                                                         host.getPort(),
                                                         host.isSSL(),
                                                         Date_t::now() + dnsRecords.front().second);
    return currResolvedHost;
}

SockAddr LDAPResolvedHost::serializeSockAddr() const {
    return SockAddr::create(_address, _port, AF_UNSPEC);
}

HostAndPort LDAPResolvedHost::serializeHostAndPort() const {
    return HostAndPort(_hostName, _port);
}
}  // namespace mongo
