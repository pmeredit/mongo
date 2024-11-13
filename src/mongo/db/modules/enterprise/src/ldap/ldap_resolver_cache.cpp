/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "ldap_resolver_cache.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "mongo/logv2/log.h"
#include "mongo/util/dns_query.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kConnectionPool


namespace mongo {

namespace {
int getDnsPortNumber(const std::vector<std::pair<dns::SRVHostEntry, Seconds>>& dnsRecords,
                     const LDAPHost& host) {
    int dnsPort = dnsRecords.front().first.port;
    if (host.isSSL()) {
        if (dnsPort == 389) {
            dnsPort = 636;
        } else if (dnsPort == 3268) {
            dnsPort = 3269;
        }
    }
    return dnsPort;
}
}  // namespace

LDAPResolvedHost LDAPDNSResolverCache::_getResolvedHost(const LDAPHost& host,
                                                        const CacheEntry& entry) {
    return LDAPResolvedHost(host.getName(),
                            entry.name,
                            host.getType() == LDAPHost::Type::kDefault ? host.getPort()
                                                                       : entry.port,
                            host.isSSL());
}

StatusWith<LDAPResolvedHost> LDAPDNSResolverCache::resolve(const LDAPHost& host) {
    if (host.isIpvSix() || host.isIpvFour()) {
        LDAPResolvedHost currHost =
            LDAPResolvedHost(host.getName(), host.getName(), host.getPort(), host.isSSL());
        return currHost;
    }

    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        auto cachedHost = _dnsCache.find({host.getType(), host.getName()});
        if (cachedHost != _dnsCache.end() && !(cachedHost->second.expiration <= Date_t::now())) {
            return _getResolvedHost(host, cachedHost->second);
        }
    }

    try {
        // Drop lock while going to the network
        auto resolvedHost = _resolveHost(host);

        {
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            _dnsCache.insert({{host.getType(), host.getName()}, resolvedHost});
            return _getResolvedHost(host, resolvedHost);
        }
    }

    catch (ExceptionFor<ErrorCodes::DNSHostNotFound>&) {
        std::string errorMsg("LDAP Host: " + host.getName() + " was NOT successfully resolved.");
        return Status(ErrorCodes::DNSHostNotFound, errorMsg);
    }
}

LDAPDNSResolverCache::CacheEntry LDAPDNSResolverCache::_resolveHost(const LDAPHost& host) {
    if (host.getType() == LDAPHost::Type::kSRVRaw) {
        std::vector<std::pair<dns::SRVHostEntry, Seconds>> dnsRecords =
            dns::lookupSRVRecords(host.getName());
        int dnsPort = getDnsPortNumber(dnsRecords, host);
        return CacheEntry{
            dnsRecords.front().first.host, dnsPort, Date_t::now() + dnsRecords.front().second};
    } else if (host.getType() == LDAPHost::Type::kSRV) {
        /*
        Under SRV mode, probe for
        _ldap._tcp.gc_msdcs.<DNSDomainName>
        then
        _ldap._tcp.<DNSDomainName>
        */

        std::string gcName("_ldap._tcp.gc._msdcs." + host.getName());
        try {
            std::vector<std::pair<dns::SRVHostEntry, Seconds>> dnsRecords =
                dns::lookupSRVRecords(gcName);
            int dnsPort = getDnsPortNumber(dnsRecords, host);
            return CacheEntry{
                dnsRecords.front().first.host, dnsPort, Date_t::now() + dnsRecords.front().second};
        } catch (ExceptionFor<ErrorCodes::DNSHostNotFound>&) {
            // "Could not find Active Directory Global Catalog SRV record, host {host}",
            LOGV2_DEBUG(5904800,
                        3,
                        "Could not find Active Directory Global Catalog SRV record",
                        "host"_attr = gcName);
        } catch (ExceptionFor<ErrorCodes::DNSProtocolError>&) {
            // "Could not find Active Directory Global Catalog SRV record, host {host}",
            LOGV2_DEBUG(6771401,
                        3,
                        "Could not find Active Directory Global Catalog SRV record",
                        "host"_attr = gcName);
        }

        std::vector<std::pair<dns::SRVHostEntry, Seconds>> dnsRecords =
            dns::lookupSRVRecords("_ldap._tcp." + host.getName());
        int dnsPort = getDnsPortNumber(dnsRecords, host);
        return CacheEntry{
            dnsRecords.front().first.host, dnsPort, Date_t::now() + dnsRecords.front().second};
    } else {
        std::vector<std::pair<std::string, Seconds>> dnsRecords =
            dns::lookupARecords(host.getName());
        return CacheEntry{host.getName(), -1, Date_t::now() + dnsRecords.front().second};
    }
}

LDAPHost LDAPResolvedHost::toLDAPHost() const {
    return LDAPHost(LDAPHost::Type::kDefault, HostAndPort(_address, _port), _isSSL);
}

SockAddr LDAPResolvedHost::serializeSockAddr() const {
    return SockAddr::create(_address, _port, AF_UNSPEC);
}

HostAndPort LDAPResolvedHost::serializeHostAndPort() const {
    return HostAndPort(_hostName, _port);
}
}  // namespace mongo
