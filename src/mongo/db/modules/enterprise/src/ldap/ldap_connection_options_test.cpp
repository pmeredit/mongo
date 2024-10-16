/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#ifdef _WIN32
// clang-format off
#include <winldap.h>
#include <winber.h>  // winldap.h must be included before
// clang-format on
#else
#include <ldap.h>
#endif

#include "mongo/unittest/unittest.h"

#include "connections/ldap_connection_helpers.h"
#include "ldap_connection_options.h"


namespace mongo {
namespace {

TEST(LDAPBindTypeTests, CanStringifySimple) {
    ASSERT_EQ("simple", authenticationChoiceToString(LDAPBindType::kSimple));
}

TEST(LDAPBindTypeTests, CanStringifySasl) {
    ASSERT_EQ("sasl", authenticationChoiceToString(LDAPBindType::kSasl));
}

TEST(LDAPBindTypeTests, CanParseSimple) {
    auto result = getLDAPBindType("simple");
    ASSERT(result.isOK() && result.getValue() == LDAPBindType::kSimple);
}

TEST(LDAPBindTypeTests, CanParseSasl) {
    auto result = getLDAPBindType("sasl");
    ASSERT(result.isOK() && result.getValue() == LDAPBindType::kSasl);
}

TEST(LDAPBindTypeTests, CanNotParseBadValue) {
    auto result = getLDAPBindType("badValue");
    ASSERT_FALSE(result.isOK());
}

TEST(ParseHostURIs, EmptyString) {
    auto result = LDAPConnectionOptions::parseHostURIs("");
    ASSERT_TRUE(result.isOK());
    ASSERT(result.getValue().empty());
}

TEST(ParseHostURIs, SingleHost) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(size_t(1), result.getValue().size());
    ASSERT_EQ("first.example", result.getValue()[0].getName());
}

TEST(ParseHostURIs, HostWithProtocol) {
    auto result = LDAPConnectionOptions::parseHostURIs("ldap://first.example");
    ASSERT_FALSE(result.isOK());
}

TEST(ParseHostURIs, TwoSpaceSeparatedHosts) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example second.example");
    ASSERT_FALSE(result.isOK());
}

TEST(ParseHostURIs, TwoCommaSeparatedHosts) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example,second.example");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ(size_t(2), result.getValue().size());
    ASSERT_EQ("first.example", result.getValue()[0].getName());
    ASSERT_EQ("second.example", result.getValue()[1].getName());
}

TEST(ParseHostURIs, TwoCommaSeparatedHostsWithProtocol) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example,ldaps://second.example");
    ASSERT_FALSE(result.isOK());
}

TEST(SingleHostTLS, LDAPHostParsing) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example:1234", true);
    ASSERT_EQ(size_t(1), result.getValue().size());
    ASSERT_EQ("first.example", result.getValue()[0].getName());
}

TEST(SingleHostTLS, LDAPHostCustomPortSSL) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example:1234", true);
    ASSERT_EQ(1234, result.getValue()[0].getPort());
}

TEST(SingleHostTLS, LDAPHostCustomPortNoSSL) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example:1234", false);
    ASSERT_EQ(1234, result.getValue()[0].getPort());
}

TEST(SingleHostTLS, LDAPHostDefaultSSLPort) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example", true);
    ASSERT_EQ(636, result.getValue()[0].getPort());
}

TEST(SingleHostTLS, LDAPHostDefaultNoSSLPort) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example", false);
    ASSERT_EQ(389, result.getValue()[0].getPort());
}

TEST(SingleHostTLS, LDAPHostURISSL) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example:1234", true);
    ASSERT_EQ("ldaps://first.example:1234", result.getValue()[0].serializeURI());
}

TEST(SingleHostTLS, LDAPHostURINoSSL) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example:1234", false);
    ASSERT_EQ("ldap://first.example:1234", result.getValue()[0].serializeURI());
}

TEST(SingleHostTLS, LDAPHostIpvSixNoPort) {
    auto result = LDAPConnectionOptions::parseHostURIs("[1234:5678:9000:0000:0000]", false);
    ASSERT_EQ("ldap://[1234:5678:9000:0000:0000]:389", result.getValue()[0].serializeURI());
}

TEST(SingleHostTLS, LDAPHostIpvSixPort) {
    auto result = LDAPConnectionOptions::parseHostURIs("[1234:5678:9000:0000:0000]:1234", false);
    ASSERT_EQ("ldap://[1234:5678:9000:0000:0000]:1234", result.getValue()[0].serializeURI());
}

TEST(SingleHostTLS, LDAPHostShortLocalhost) {
    auto result = LDAPConnectionOptions::parseHostURIs("[::1]", false);
    ASSERT_EQ("ldap://[::1]:389", result.getValue()[0].serializeURI());
}

TEST(SingleHostTLS, SRV) {
    auto result = LDAPConnectionOptions::parseHostURIs("srv:first.example:1234", true);
    ASSERT_EQ("ldaps://first.example:1234", result.getValue()[0].serializeURI());
    ASSERT(LDAPHost::Type::kSRV == result.getValue()[0].getType());
}

TEST(SingleHostTLS, SRVRaw) {
    auto result = LDAPConnectionOptions::parseHostURIs("srv_raw:first.example:1234", true);
    ASSERT_EQ("ldaps://first.example:1234", result.getValue()[0].serializeURI());
    ASSERT(LDAPHost::Type::kSRVRaw == result.getValue()[0].getType());
}

// Test: Active Directory may return an array with a single null value so verify the iterator works
// and treats this case as an empty array
TEST(LdapArrayTest, VerifyEquality) {

    std::vector<berval*> values;
    values.push_back(nullptr);

    ASSERT_TRUE(LDAPArrayIterator<berval*>(values.data()) == LDAPArrayIterator<berval*>(nullptr));
}

}  // namespace
}  // namespace mongo
