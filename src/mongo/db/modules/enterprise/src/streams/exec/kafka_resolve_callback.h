#pragma once

#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>

#include "mongo/stdx/condition_variable.h"

namespace streams {

struct Context;

// KafkaResolveCallback is used to resolve names when VPC peering is enabled.
// This code path replaces the normally resolved (via the OS resolver) names
// with the appropriate names of the Gateway Proxy endpoints for this particular
// tenant's deployment, which then routes the traffic using the TLS SNI
// names passed in the TLS client hello message.  This should only be used
// (in general) for TLS enabled connections, as this won't do anything
// useful for plaintext connections.
class KafkaResolveCallback : public RdKafka::ResolveCb {
public:
    KafkaResolveCallback(Context* context, std::string operatorName, std::string targetProxy);
    // Called by librdkafka.
    int resolve_cb(const char* node,
                   const char* service,
                   const struct addrinfo* hints,
                   struct addrinfo** res) override;

private:
    Context* _context{nullptr};
    std::pair<std::string, std::string> splitAddressAndService(std::string& hostname);
    sockaddr_in resolve_name(const std::string& hostname, const std::string& port);
    int resolveCbImpl(const char* node,
                      const char* service,
                      const struct addrinfo* hints,
                      struct addrinfo** res);

    std::string _operatorName;
    std::string _targetProxy;
    std::unique_ptr<addrinfo> _endpoint;
};


}  // namespace streams
