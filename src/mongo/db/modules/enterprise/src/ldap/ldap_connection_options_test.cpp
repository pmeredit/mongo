/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/unittest/unittest.h"

#include "ldap_connection_options.h"

namespace mongo {

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
    ASSERT_EQ("first.example", result.getValue()[0]);
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
    ASSERT_EQ("first.example", result.getValue()[0]);
    ASSERT_EQ("second.example", result.getValue()[1]);
}

TEST(ParseHostURIs, TwoCommaSeparatedHostsWithProtocol) {
    auto result = LDAPConnectionOptions::parseHostURIs("first.example,ldaps://second.example");
    ASSERT_FALSE(result.isOK());
}


}  // namespace mongo
