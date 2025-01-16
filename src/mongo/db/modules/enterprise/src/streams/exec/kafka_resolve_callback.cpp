/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <arpa/inet.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <netinet/in.h>

#include "mongo/util/assert_util.h"
#include "streams/exec/context.h"
#include "streams/exec/kafka_resolve_callback.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

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

    // Initialize random number generator for random GWProxy server selection method.
    std::random_device _rd;
    std::mt19937 _gen(_rd());
}

std::pair<std::string, std::string> KafkaResolveCallback::generateAddressAndService(
    const std::string& hostname, const char* service) {
    // TODO(STREAMS-974): This only supports AF_INET (eg. no IPv6).
    std::vector<std::string> result;
    boost::algorithm::split(result, hostname, boost::algorithm::is_any_of(":"));

    if (result.size() == 1 && service != nullptr) {
        // Use provided TCP port passed in as service hint in original query.
        result.push_back(std::string{service});
    }

    uassert(ErrorCodes::InternalError,
            str::stream() << "Unable to determine port for proxyEndpoint: " << hostname
                          << " Actual size of array is: " << result.size(),
            result.size() == 2);

    LOGV2_INFO(780098,
               "Generated proxy endpoint for connection",
               "context"_attr = _context,
               "hostname"_attr = result[0],
               "port"_attr = result[1]);

    return {result[0], result[1]};
}

struct in_addr KafkaResolveCallback::getRandomProxy(AddrInfoPtr& addresses) {
    std::vector<struct in_addr> proxyEndpoints;

    for (addrinfo* ai = addresses.get(); ai != nullptr; ai = ai->ai_next) {
        struct in_addr address = reinterpret_cast<sockaddr_in*>(ai->ai_addr)->sin_addr;

        char ipAddress[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &address, ipAddress, sizeof(ipAddress));

        LOGV2_INFO(780097,
                   "Adding GWProxy address to pool",
                   "ipAddress"_attr = ipAddress,
                   "context"_attr = _context);

        proxyEndpoints.push_back(address);
    }

    if (proxyEndpoints.empty()) {
        addUserFacingError("No proxy IPs available - connection currently unavailable");
        uasserted(ErrorCodes::InternalError, "No GWProxy IPs available");
    }

    // Choose a GWProxy endpoint at random. This is pretty low performance,
    // but is out of the fast path, and does not get called very often.
    std::uniform_int_distribution<int> rng(0, proxyEndpoints.size() - 1);
    return proxyEndpoints.at(rng(_gen));
}

std::unique_ptr<sockaddr_in> KafkaResolveCallback::resolve_name(const std::string& hostname,
                                                                const std::string& port) {
    int err;

    // Build hints for getaddrinfo.
    addrinfo hints{};
    hints.ai_flags = AI_ADDRCONFIG;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    // Resolve hostname of proxy.
    addrinfo* tempAddrInfoPtr = nullptr;
    err = getaddrinfo(hostname.c_str(), port.c_str(), &hints, &tempAddrInfoPtr);
    auto addrs = AddrInfoPtr{tempAddrInfoPtr, freeaddrinfo};
    if (err != 0) {
        addUserFacingError("Unable to resolve proxy name - connection currently unavailable");
        uasserted(ErrorCodes::InvalidOptions,
                  str::stream() << "Unable to resolve proxy name provided.  Hostname: " << hostname
                                << "Port: " << port);
    }

    // Construct a new sockaddr_in with our custom port.
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(atoi(port.c_str()));
    addr.sin_addr = getRandomProxy(addrs);

    return std::make_unique<sockaddr_in>(addr);
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
        delete (*res)->ai_addr;
        delete *res;

        return 0;
    }

    LOGV2_INFO(780008, "Calling custom DNS resolver for librdkafka.", "context"_attr = _context);

    auto [hostname, port] = generateAddressAndService(_targetProxy, service);
    auto resolverResult = resolve_name(hostname, port);

    // Using smart pointers for these is a bit superfluous, but limits the number
    // of places we specifically create raw pointers and makes it more obvious
    // when we transfer ownership of them to the C callback code in librdkafka.
    std::unique_ptr<addrinfo> endpoint = std::make_unique<addrinfo>();
    endpoint->ai_family = AF_INET;
    endpoint->ai_socktype = SOCK_STREAM;
    endpoint->ai_protocol = IPPROTO_TCP;
    endpoint->ai_addrlen = sizeof(sockaddr_in);
    endpoint->ai_next = nullptr;
    endpoint->ai_addr = reinterpret_cast<sockaddr*>(resolverResult.release());

    // Point res to the underlying raw pointer. The addrinfo struct will be cleaned
    // up when the operator which instantiated the resolve_cb is destroyed.
    *res = endpoint.release();

    return 0;
}

}  // namespace streams
