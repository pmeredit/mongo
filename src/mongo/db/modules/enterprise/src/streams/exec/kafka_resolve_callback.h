/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <random>
#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>

#include "streams/exec/kafka_callback_base.h"

namespace {
using AddrInfoPtr = std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)>;
}
namespace streams {

struct Context;

// KafkaResolveCallback is used to resolve names when VPC peering is enabled.
// This code path replaces the normally resolved (via the OS resolver) names
// with the appropriate names of the Gateway Proxy endpoints for this particular
// tenant's deployment, which then routes the traffic using the TLS SNI
// names passed in the TLS client hello message.  This should only be used
// (in general) for TLS enabled connections, as this won't do anything
// useful for plaintext connections.
class KafkaResolveCallback : public RdKafka::ResolveCb, public KafkaCallbackBase {
public:
    KafkaResolveCallback(Context* context, std::string operatorName, std::string targetProxy);
    // Called by librdkafka.
    int resolve_cb(const char* node,
                   const char* service,
                   const struct addrinfo* hints,
                   struct addrinfo** res) override;

private:
    friend class KafkaResolveCallbackTest;

    Context* _context{nullptr};
    std::pair<std::string, std::string> generateAddressAndService(const std::string& hostname,
                                                                  const char* service);
    sockaddr_in resolve_name(const std::string& hostname, const std::string& port);
    struct in_addr getRandomProxy(AddrInfoPtr& addresses);
    int resolveCbImpl(const char* node,
                      const char* service,
                      const struct addrinfo* hints,
                      struct addrinfo** res);

    std::string _operatorName;
    std::string _targetProxy;
    std::unique_ptr<addrinfo> _endpoint;
    std::mt19937 _gen;
};


}  // namespace streams
