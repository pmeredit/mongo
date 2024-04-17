/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/kafka_resolve_callback.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace {
using AddrInfoPtr = std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)>;
}
namespace streams {

KafkaResolveCallback::KafkaResolveCallback(Context* context,
                                           const std::string operatorName,
                                           const std::string targetProxy)
    : _context(context), _operatorName(std::move(operatorName)), _targetProxy(targetProxy) {
    LOGV2_INFO(780006,
               "Enabling custom resolver",
               "context"_attr = _context,
               "operatorName"_attr = _operatorName,
               "targetProxy"_attr = _targetProxy);
}

std::pair<std::string, std::string> KafkaResolveCallback::splitAddressAndService(
    std::string& hostname) {
    // TODO(STREAMS-974): This only supports AF_INET (eg. no IPv6).
    std::vector<std::string> result;
    boost::algorithm::split(result, hostname, boost::algorithm::is_any_of(":"));

    uassert(ErrorCodes::InternalError,
            str::stream() << "Invalid hostname:port combination for proxyEndpoint: " << hostname
                          << " Actual size of array is: " << result.size(),
            result.size() == 2);

    return {result[0], result[1]};
}

sockaddr_in KafkaResolveCallback::resolve_name(const std::string& hostname,
                                               const std::string& port) {
    int err;

    // Build hints for getaddrinfo.
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    // Resolve hostname of proxy.
    addrinfo* tempAddrInfoPtr = nullptr;
    err = getaddrinfo(hostname.c_str(), port.c_str(), &hints, &tempAddrInfoPtr);
    auto addrs = AddrInfoPtr{tempAddrInfoPtr, freeaddrinfo};
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Unable to resolve proxy name provided.  Hostname: " << hostname
                          << "Port: " << port,
            err == 0);

    // Construct a new sockaddr_in with our custom port.
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(atoi(port.c_str()));
    addr.sin_addr = reinterpret_cast<sockaddr_in*>(addrs->ai_addr)->sin_addr;

    return addr;
}

// resolve_cb wraps resolveCbImpl to handle any unexpected exceptions.
int KafkaResolveCallback::resolve_cb(const char* node,
                                     const char* service,
                                     const struct addrinfo* hints,
                                     struct addrinfo** res) {
    try {
        return resolveCbImpl(node, service, hints, res);
    } catch (const std::exception& e) {
        LOGV2_ERROR(780051,
                    "Unexpected exception while running resolve_cb in librdkafka",
                    "exception"_attr = e.what());
    }
    return -1;
}

// resolveCbImpl implemements a custom DNS resolver for rdkafka to allow us
// to selectively route customer traffic to internal GWProxy endpoints
// for VPC peering/private networking.
int KafkaResolveCallback::resolveCbImpl(const char* node,
                                        const char* service,
                                        const struct addrinfo* hints,
                                        struct addrinfo** res) {
    // If we call this with a null node and service parameter, this means we're
    // cleaning up (the same way a call to getaddrinfo does).
    if ((!node && !service) && *res) {
        LOGV2_INFO(780007,
                   "resolver has null node and service, freeing structures",
                   "context"_attr = _context);
        _endpoint.reset();
        return 0;
    }

    LOGV2_INFO(780008, "Calling custom DNS resolver for librdkafka.", "context"_attr = _context);

    auto [hostname, port] = splitAddressAndService(_targetProxy);
    auto resolverResult = resolve_name(hostname, port);

    _endpoint = std::make_unique<addrinfo>();
    _endpoint->ai_family = AF_INET;
    _endpoint->ai_socktype = SOCK_STREAM;
    _endpoint->ai_protocol = IPPROTO_TCP;
    _endpoint->ai_addrlen = sizeof(sockaddr_in);
    _endpoint->ai_next = nullptr;
    _endpoint->ai_addr = reinterpret_cast<sockaddr*>(&resolverResult);

    // Point res to the underlying raw pointer. The addrinfo struct will be cleaned
    // up when the operator which instantiated the resolve_cb is destroyed.
    *res = _endpoint.get();

    return 0;
}

}  // namespace streams
