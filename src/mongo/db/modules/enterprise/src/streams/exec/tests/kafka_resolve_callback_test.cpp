/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <arpa/inet.h>

#include "mongo/unittest/assert.h"
#include "mongo/unittest/framework.h"
#include "streams/exec/kafka_resolve_callback.h"
#include "streams/exec/tests/test_utils.h"

using namespace mongo;

namespace streams {

class KafkaResolveCallbackTest : public unittest::Test {
protected:
    auto checkResolverFailures();
    auto testHostnameParser(std::string& fqdnAndPort, const char* service);
    auto makeContext() {
        auto context = std::make_unique<Context>();
        context->streamProcessorId = UUID::gen().toString();
        context->tenantId = UUID::gen().toString();

        return context;
    }
};

auto KafkaResolveCallbackTest::checkResolverFailures() {
    auto context = makeContext();
    KafkaResolveCallback krc{context.get(), "testOperator", "an.unresolvable.name.local:12345"};

    auto host = StringData("another.unresolvable.name.local");
    auto port = StringData("23456");

    // This should throw a DBException.
    return krc.resolve_name(host.toString(), port.toString());
}

auto KafkaResolveCallbackTest::testHostnameParser(std::string& fqdnAndPort, const char* service) {
    auto context = makeContext();
    KafkaResolveCallback krc{context.get(), "testOperator", fqdnAndPort};

    return krc.generateAddressAndService(fqdnAndPort, service);
}

TEST_F(KafkaResolveCallbackTest, EnsureResolverWorksWithValidTargetHost) {
    // Test to ensure that if we resolve a normal Kafka looking name, that
    // we actually return the specific proxy endpoint successfully instead.
    auto context = makeContext();
    KafkaResolveCallback krc{context.get(), "testOperator", "localhost:30000"};

    std::string node{"foobarnode"};
    std::string service{"foobarservice"};

    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    struct addrinfo* res;

    krc.resolve_cb(node.c_str(), service.c_str(), &hints, &res);
    auto addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
    char saddr[INET_ADDRSTRLEN];

    inet_ntop(AF_INET, &addr.s_addr, saddr, INET_ADDRSTRLEN);
    ASSERT_EQUALS(saddr, StringData("127.0.0.1"));

    // Free structures
    krc.resolve_cb(nullptr, nullptr, nullptr, &res);
}

TEST_F(KafkaResolveCallbackTest, EnsureResolverWorksWithValidTargetIP) {
    // Ensure that if we pass an IP address in as the target, that the
    // resolver returns that.
    auto context = makeContext();
    KafkaResolveCallback krc{context.get(), "testOperator", "1.2.3.4:30000"};

    std::string node{"foobarnode"};
    std::string service{"foobarservice"};

    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    struct addrinfo* res;

    krc.resolve_cb(node.c_str(), service.c_str(), &hints, &res);
    auto addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
    char saddr[INET_ADDRSTRLEN];

    inet_ntop(AF_INET, &addr.s_addr, saddr, INET_ADDRSTRLEN);
    ASSERT_EQUALS(saddr, StringData("1.2.3.4"));

    // Free structures
    krc.resolve_cb(nullptr, nullptr, nullptr, &res);
}

TEST_F(KafkaResolveCallbackTest, EnsureResolverFailsPredictablyBadHostname) {
    // Wrapper to test private resolve_name method with invalid data, to ensure
    // we throw an exception correctly.
    ASSERT_THROWS(checkResolverFailures(), mongo::DBException);
}

TEST_F(KafkaResolveCallbackTest, TestGoodHostAndPortPair) {
    // Wrapper to test private generateAddressAndService method.
    // This should return a correctly split hostname and port.
    std::string hostAndPort{"foo.bar.name:12345"};

    auto splitHost = testHostnameParser(hostAndPort, nullptr);
    ASSERT_EQUALS(splitHost.first, "foo.bar.name");
    ASSERT_EQUALS(splitHost.second, "12345");
}

TEST_F(KafkaResolveCallbackTest, TestBadHostAndPortPair) {
    // Wrapper to test private generateAddressAndService method.  This
    // should throw an exception, as we can't determine a valid port
    // to connect to on the proxy.
    std::string hostAndPort{"foo.bar.name.no.port"};

    ASSERT_THROWS(testHostnameParser(hostAndPort, nullptr), mongo::DBException);
}

TEST_F(KafkaResolveCallbackTest, TestGoodHostAndService) {
    // Wrapper to test private generateAddressAndService method.
    // This should determine that there is no embedded port in the
    // target proxy hostname, and use the provided one passed in to
    // the original resolve_cb call.
    std::string hostAndPort{"foo.bar.name.no.port"};

    auto splitHost = testHostnameParser(hostAndPort, "12345");
    ASSERT_EQUALS(splitHost.first, "foo.bar.name.no.port");
    ASSERT_EQUALS(splitHost.second, "12345");
}

}  // namespace streams
