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
    ASSERT_EQ("", result.getValue());
}

TEST(ParseHostURIs, SingleLDAPHost) {
    auto result = LDAPConnectionOptions::parseHostURIs("ldap://first.example");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ("ldap://first.example", result.getValue());
}

TEST(ParseHostURIs, SingleLDAPSHost) {
    auto result = LDAPConnectionOptions::parseHostURIs("ldaps://first.example");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ("ldaps://first.example", result.getValue());
}

TEST(ParseHostURIs, BadLDAPProtocol) {
    auto result = LDAPConnectionOptions::parseHostURIs("mongo://first.example");
    ASSERT_FALSE(result.isOK());
}

TEST(ParseHostURIs, TwoSpaceSeparatedHosts) {
    auto result =
        LDAPConnectionOptions::parseHostURIs("ldap://first.example ldap://second.example");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ("ldap://first.example ldap://second.example", result.getValue());
}

TEST(ParseHostURIs, TwoCommaSeparatedHosts) {
    auto result =
        LDAPConnectionOptions::parseHostURIs("ldap://first.example,ldap://second.example");
    ASSERT_TRUE(result.isOK());
    ASSERT_EQ("ldap://first.example ldap://second.example", result.getValue());
}

TEST(ParseHostURIs, TwoCommaSeparatedHostsWithBadProtocol) {
    auto result =
        LDAPConnectionOptions::parseHostURIs("ldap://first.example,mongo://second.example");
    ASSERT_FALSE(result.isOK());
}


}  // namespace mongo
