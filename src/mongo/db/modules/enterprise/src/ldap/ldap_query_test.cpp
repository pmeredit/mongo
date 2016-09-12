/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/unittest/unittest.h"

#include "ldap_connection_options.h"
#include "ldap_query.h"
#include "mongo/stdx/memory.h"

namespace mongo {

namespace {

const std::string userDN{"cn=sajack,dc=mongodb,dc=com"};
const LDAPAttributeKeys requestedAttributes{"email", "uid"};

TEST(LDAPQueryInstantiate, InstantiationFromRawStringAlwaysSucceed) {
    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfig(userDN);
    ASSERT_OK(swQueryParameters.getStatus());

    auto swQuery = LDAPQuery::instantiateQuery(swQueryParameters.getValue());
    ASSERT_OK(swQuery.getStatus());
    ASSERT_EQ("cn=sajack,dc=mongodb,dc=com", swQuery.getValue().getBaseDN());
}


TEST(LDAPQueryInstantiate, InstantiationFromUserConfigAndUserNameSucceeds) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithUserName("cn={USER},dc=mongodb,dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    auto swQuery = LDAPQuery::instantiateQuery(swQueryParameters.getValue(), "sajack");
    ASSERT_OK(swQuery.getStatus());
    ASSERT_EQ("cn=sajack,dc=mongodb,dc=com", swQuery.getValue().getBaseDN());
}

TEST(LDAPQueryInstantiate, InstantiationFromUserConfigAndUserNameWithBackslashSucceeds) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithUserName("cn={USER},dc=mongodb,dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    auto swQuery = LDAPQuery::instantiateQuery(swQueryParameters.getValue(), "jack\\,sa");
    ASSERT_OK(swQuery.getStatus());
    ASSERT_EQ("cn=jack\\,sa,dc=mongodb,dc=com", swQuery.getValue().getBaseDN());
}


TEST(LDAPQueryInstantiate, InstantiationFromEmptyComponentsConfigAndNoTokensSucceeds) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithComponents("cn=sajack,dc=mongodb,dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    auto swQuery = LDAPQuery::instantiateQuery(swQueryParameters.getValue(), {});
    ASSERT_OK(swQuery.getStatus());
    ASSERT_EQ("cn=sajack,dc=mongodb,dc=com", swQuery.getValue().getBaseDN());
}

TEST(LDAPQueryInstantiate, InstantiationFromComponentConfigAndTokenSucceeds) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithComponents("cn={0},dc=mongodb,dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    auto swQuery = LDAPQuery::instantiateQuery(swQueryParameters.getValue(), {"sajack"});
    ASSERT_OK(swQuery.getStatus());
    ASSERT_EQ("cn=sajack,dc=mongodb,dc=com", swQuery.getValue().getBaseDN());
}

TEST(LDAPQueryInstantiate, InstantiationFromComponentConfigWithMoreComponentsThanTokensFails) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithComponents("cn={0},dc=mongodb,dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    ASSERT_NOT_OK(
        LDAPQuery::instantiateQuery(swQueryParameters.getValue(), {"sajack", "tooMuchData"}));
}

TEST(LDAPQueryInstantiate, InstantiationFromComponentConfigWithMoreTokensThanComponentsSucceeds) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithComponents("cn={0},dc={1},dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    ASSERT_OK(LDAPQuery::instantiateQuery(swQueryParameters.getValue(), {"sajack"}));
}

TEST(LDAPQueryInstantiate, InstantiationFromComponentConfigWithComponentAndRepeatedTokenSucceeds) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithComponents("cn={0},dc={0},dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    auto swQuery = LDAPQuery::instantiateQuery(swQueryParameters.getValue(), {"sajack"});
    ASSERT_OK(swQuery.getStatus());
    ASSERT_EQ("cn=sajack,dc=sajack,dc=com", swQuery.getValue().getBaseDN());
}

TEST(LDAPQueryInstantiate, InstantiationFromComponentConfigWithMissingTokenFails) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithComponents("cn={0},dc={2},dc=com?");
    ASSERT_OK(swQueryParameters.getStatus());

    ASSERT_NOT_OK(LDAPQuery::instantiateQuery(swQueryParameters.getValue(), {"sajack", "mongodb"}));
}

TEST(LDAPQueryInstantiate, InstantiationFromComponentConfigWithEscapedBackslashSucceeds) {
    auto swQueryParameters =
        LDAPQueryConfig::createLDAPQueryConfigWithComponents("cn={0},dc=mongodb,dc=com");
    ASSERT_OK(swQueryParameters.getStatus());

    auto swQuery = LDAPQuery::instantiateQuery(swQueryParameters.getValue(), {"jack\\,sa"});
    ASSERT_OK(swQuery.getStatus());
    ASSERT_EQ("cn=jack\\,sa,dc=mongodb,dc=com", swQuery.getValue().getBaseDN());
}

std::unique_ptr<LDAPQueryConfig> createLDAPQueryConfig(std::string queryString,
                                                       bool success = true) {
    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfig(queryString);
    if (!success) {
        ASSERT_NOT_OK(swQueryParameters.getStatus());
        return std::unique_ptr<LDAPQueryConfig>(nullptr);
    }
    ASSERT_OK(swQueryParameters.getStatus());
    return stdx::make_unique<LDAPQueryConfig>(std::move(swQueryParameters.getValue()));
}

}  // namespace

TEST(LDAPQueryConfigParseTest, parseldapDN) {
    auto ldapQueryParameters = createLDAPQueryConfig(userDN);
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
}

/*TEST(LDAPQueryConfigParseTest, parseldapSlashSlashSlashDN) {
    auto ldapQueryParameters = LDAPQuery::fromString("ldaps:///cn=sajack,dc=mongodb,dc=com");
    ASSERT_OK(swldapQueryParameters->getStatus());
    auto& ldapQuery = std::get<0>(ldapQueryParameters);
    ASSERT_EQ(LDAPConnectionScheme::LDAPS, ldap.scheme);
    ASSERT_EQ(HostAndPort("", 0), ldap.ldapServer);
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
}*/

TEST(LDAPQueryConfigParseTest, parseDNQ) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
}

TEST(LDAPQueryConfigParseTest, parseldapAttr) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?email,uid");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
}

TEST(LDAPQueryConfigParseTest, parseldapSlashQQ) {
    auto ldapQueryParameters = createLDAPQueryConfig("??");
}

TEST(LDAPQueryConfigParseTest, parseldapQQ) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com??");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
}

TEST(LDAPQueryConfigParseTest, parseldapAttrQ) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?email,uid?");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
}

TEST(LDAPQueryConfigParseTest, parseldapBadScope) {
    auto ldapQueryParameters = createLDAPQueryConfig("??bad", false);
}

TEST(LDAPQueryConfigParseTest, parseldapScopeOne) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?email,uid?one");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
    ASSERT_EQ(LDAPQueryScope::kOne, ldapQueryParameters->scope);
}

TEST(LDAPQueryConfigParseTest, parseldapCaptialScopeOne) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
    ASSERT_EQ(LDAPQueryScope::kOne, ldapQueryParameters->scope);
}

TEST(LDAPQueryConfigParseTest, parseldapScopeSubtree) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?email,uid?sub");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
    ASSERT_EQ(LDAPQueryScope::kSubtree, ldapQueryParameters->scope);
}

TEST(LDAPQueryConfigParseTest, parseldapScopeBase) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?email,uid?base");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
    ASSERT_EQ(LDAPQueryScope::kBase, ldapQueryParameters->scope);
}

TEST(LDAPQueryConfigParseTest, parseldapScopeQ) {
    auto ldapQueryParameters = createLDAPQueryConfig("cn=sajack,dc=mongodb,dc=com?email,uid?one?");
    ASSERT_EQ(userDN, ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
    ASSERT_EQ(LDAPQueryScope::kOne, ldapQueryParameters->scope);
}

TEST(LDAPQueryConfigParseTest, parseldapFilter) {
    auto ldapQueryParameters = createLDAPQueryConfig("dc=mongodb,dc=com?email,uid?one?(cn=sajack)");
    ASSERT_EQ("dc=mongodb,dc=com", ldapQueryParameters->baseDN);
    ASSERT_TRUE(requestedAttributes == ldapQueryParameters->attributes);
    ASSERT_EQ(LDAPQueryScope::kOne, ldapQueryParameters->scope);
    ASSERT_EQ("(cn=sajack)", ldapQueryParameters->filter);
}

TEST(LDAPQueryConfigParseTest, parsePercentEncodedSpaces) {
    auto ldapQueryParameters = createLDAPQueryConfig("o=University%20of%20Michigan,c=US");
    ASSERT_EQ("o=University of Michigan,c=US", ldapQueryParameters->baseDN);
}

TEST(LDAPQueryConfigParseTest, parsePercentEncodedDNQuotedComma) {
    auto ldapQueryParameters = createLDAPQueryConfig("o=An%20Example%5C2C%20Inc.,c=US");
    ASSERT_EQ("o=An Example\\2C Inc.,c=US", ldapQueryParameters->baseDN);
}

TEST(LDAPQueryConfigParseTest, parsePercentEncodedFilter) {
    auto ldapQueryParameters =
        createLDAPQueryConfig("o=University%20of%20Michigan,c=US??sub?(cn=Babs%20Jensen)");
    ASSERT_EQ("o=University of Michigan,c=US", ldapQueryParameters->baseDN);
    ASSERT_EQ("(cn=Babs Jensen)", ldapQueryParameters->filter);
}

TEST(LDAPQueryConfigParseTest, parsePercentEncodedAttribute) {
    auto ldapQueryParameters = createLDAPQueryConfig(
        "o=University%20of%20Michigan,c=US?postalAddress%3f?sub?(cn=Babs%20Jensen)");
    ASSERT_EQ("o=University of Michigan,c=US", ldapQueryParameters->baseDN);
    ASSERT_EQ(static_cast<size_t>(1), ldapQueryParameters->attributes.size());
    ASSERT_EQ("postalAddress?", ldapQueryParameters->attributes[0]);
    ASSERT_EQ("(cn=Babs Jensen)", ldapQueryParameters->filter);
}


TEST(UserLDAPQueryConfigParseTest, parseUserNameQueryConfigFailsWithEmptyToken) {
    auto swQueryConfig =
        LDAPQueryConfig::createLDAPQueryConfigWithUserName("cn={},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_NOT_OK(swQueryConfig.getStatus());
}

TEST(UserLDAPQueryConfigParseTest, parseUserNameQueryConfigFailsWithNumericToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        "cn={0},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_NOT_OK(swQueryConfig.getStatus());
}

TEST(UserLDAPQueryConfigParseTest, parseUserNameQueryConfigSucceedsWithNoToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        "cn=sajack,dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_OK(swQueryConfig.getStatus());
}

TEST(UserLDAPQueryConfigParseTest, parseUserNameQueryConfigFailsWithBrokenToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        "cn={USER,dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_NOT_OK(swQueryConfig.getStatus());
}

TEST(UserLDAPQueryConfigParseTest, parseUserNameQueryConfigSucceedsWithUserToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        "cn={USER},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_OK(swQueryConfig.getStatus());
}

TEST(UserLDAPQueryConfigParseTest, parseUserNameQueryConfigSucceedsWithTwoUserTokens) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        "cn={USER},dc=mongodb,dc=com?email,uid?ONE?(cn={USER})");
    ASSERT_OK(swQueryConfig.getStatus());
}

TEST(UserLDAPQueryConfigParseTest, parseUserNameQueryConfigFailsWithInvalidTokenAfterUserToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        "cn={USER},dc=mongodb,dc=com?email,uid?ONE?(cn={0})");
    ASSERT_NOT_OK(swQueryConfig.getStatus());
}


TEST(ComponentLDAPQueryConfigParseTest, parseConfigFailsWithEmptyToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithComponents(
        "cn={},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_NOT_OK(swQueryConfig.getStatus());
}

TEST(ComponentLDAPQueryConfigParseTest, parseConfigFailsWithStringToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithComponents(
        "cn={a},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_NOT_OK(swQueryConfig.getStatus());
}

TEST(ComponentLDAPQueryConfigParseTest, parseSucceedsWithComponentToken) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithComponents(
        "cn={0},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_OK(swQueryConfig.getStatus());
}

TEST(ComponentLDAPQueryConfigParseTest, parseFailsWithComponentTokenWithNegativeID) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithComponents(
        "cn={-1},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_NOT_OK(swQueryConfig.getStatus());
}

TEST(ComponentLDAPQueryConfigParseTest, parseSucceedsWithTwoComponentTokens) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithComponents(
        "cn={0},ou={1},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_OK(swQueryConfig.getStatus());
}

TEST(ComponentLDAPQueryConfigParseTest, parseSucceedsWithTwoNonconsecutiveComponentTokens) {
    auto swQueryConfig = LDAPQueryConfig::createLDAPQueryConfigWithComponents(
        "cn={0},ou={2},dc=mongodb,dc=com?email,uid?ONE");
    ASSERT_OK(swQueryConfig.getStatus());
}


TEST(LDAPQueryConfigParsePercentTest, character) {
    auto ldapQueryParameters = createLDAPQueryConfig("a");
    ASSERT_EQ("a", ldapQueryParameters->baseDN);
}

TEST(LDAPQueryConfigParsePercentTest, percent) {
    auto ldapQueryParameters = createLDAPQueryConfig("a%", false);
}

TEST(LDAPQueryConfigParsePercentTest, percentChar) {
    auto ldapQueryParameters = createLDAPQueryConfig("a%6", false);
}

TEST(LDAPQueryConfigParsePercentTest, badEncoding) {
    auto ldapQueryParameters = createLDAPQueryConfig("a%XX", false);
}

TEST(LDAPQueryConfigParsePercentTest, percentCharChar) {
    auto ldapQueryParameters = createLDAPQueryConfig("a%61");
    ASSERT_EQ("aa", ldapQueryParameters->baseDN);
}

TEST(LDAPQueryConfigParsePercentTest, twoPercentEncodedCharacters) {
    auto ldapQueryParameters = createLDAPQueryConfig("%61%61");
    ASSERT_EQ("aa", ldapQueryParameters->baseDN);
}


}  // namespace mongo
