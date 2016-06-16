/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <functional>
#include <memory>
#include <utility>

#include "../ldap_query.h"
#include "../ldap_runner_mock.h"
#include "internal_to_ldap_user_name_mapper.h"
#include "ldap_rewrite_rule.h"
#include "mongo/base/init.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"
#include "regex_rewrite_rule.h"
#include "rewrite_rule.h"

namespace mongo {

namespace {
const std::string kEngineeringDN = "ou=engineering,dc=mongodb,dc=com";
const std::string kMarketingDN = "ou=marketing,dc=mongodb,dc=com";
const std::string kEngineeringRealmRegex = "(.+)@ENGINEERING";

}  // namespace

template <typename T, typename... Args>
T assertCreateOK(Args&&... args) {
    StatusWith<T> swRule = T::create(std::forward<Args>(args)...);
    ASSERT_OK(swRule.getStatus());
    return std::move(swRule.getValue());
}

template <typename T, typename... Args>
std::unique_ptr<T> assertCreateOKPtr(Args&&... args) {
    return stdx::make_unique<T>(assertCreateOK<T>(std::forward<Args>(args)...));
}

class LDAPTransformContext : public unittest::Test {
public:
    LDAPTransformContext() : runner(new LDAPRunnerMock()) {}

protected:
    LDAPRunnerMock* runner;
};

using RegexTransformTest = LDAPTransformContext;
using LDAPTransformTest = LDAPTransformContext;
using NameMapperTest = LDAPTransformContext;

TEST_F(RegexTransformTest, matchesAString) {
    auto regex = assertCreateOK<RegexRewriteRule>("[a-zA-Z]+", "");
    ASSERT_OK(regex.resolve(runner, "helloWorld"));
}

TEST_F(RegexTransformTest, failsToMatchAString) {
    auto regex = assertCreateOK<RegexRewriteRule>("[a-zA-Z]+", "");
    ASSERT_NOT_OK(regex.resolve(runner, "555"));
}

TEST_F(RegexTransformTest, substitutesAString) {
    auto regex = assertCreateOK<RegexRewriteRule>("([a-zA-Z]+)", "Got: {0}");
    auto result = regex.resolve(runner, "helloWorld");
    ASSERT_OK(result);
    ASSERT_EQ("Got: helloWorld", result.getValue());
}

TEST_F(RegexTransformTest, substitutesTwoStrings) {
    auto regex = assertCreateOK<RegexRewriteRule>("([a-zA-Z]+) ([a-zA-Z]+)", "Got: {0}. And: {1}.");
    auto result = regex.resolve(runner, "helloWorld goodbyeWorld");
    ASSERT_OK(result);
    ASSERT_EQ("Got: helloWorld. And: goodbyeWorld.", result.getValue());
}

TEST_F(RegexTransformTest, substitutesMultidigitCaptureGroups) {
    std::stringstream regexRule("");          // ([0-9]+) ([0-9]+) ... ([0-9]+)
    std::stringstream regexInput("");         // 0 1 2 ... 10
    std::stringstream regexSubstitution("");  // Rule#{0} Rule#{1} ... Rule#{10}
    std::stringstream regexOutput("");        // Rule#0 Rule#1 ... Rule#10
    for (int i = 0; i < 11; ++i) {
        regexRule << "([0-9]+)";
        regexInput << i;
        regexSubstitution << "Rule#{" << i << "}";
        regexOutput << "Rule#" << i;
        if (i != 10) {
            regexRule << " ";
            regexInput << " ";
            regexSubstitution << " ";
            regexOutput << " ";
        }
    }

    auto regex = assertCreateOK<RegexRewriteRule>(regexRule.str(), regexSubstitution.str());
    auto result = regex.resolve(runner, regexInput.str());
    ASSERT_OK(result);
    ASSERT_EQ(regexOutput.str(), result.getValue());
}

TEST_F(LDAPTransformTest, stringSubstitutionFailureReturnsBadStatus) {
    auto transformer =
        assertCreateOK<LDAPRewriteRule>(kEngineeringRealmRegex, "cn={0}" + kEngineeringDN);
    ASSERT_NOT_OK(transformer.resolve(runner, "sajack@MARKETING"));
}

TEST_F(LDAPTransformTest, stringSubstitutionToBadLDAPFailsToCompile) {
    ASSERT_NOT_OK(
        LDAPRewriteRule::create(kEngineeringRealmRegex, kEngineeringDN + "??invalidScope?(uid={0})")
            .getStatus());
}

TEST_F(LDAPTransformTest, noResults) {
    runner->push(kEngineeringDN + "??one?(uid=sajack)",
                 StatusWith<LDAPEntityCollection>{LDAPEntityCollection{}});

    auto transformer =
        assertCreateOK<LDAPRewriteRule>(kEngineeringRealmRegex, kEngineeringDN + "??one?(uid={0})");
    auto result = transformer.resolve(runner, "sajack@ENGINEERING");
    ASSERT_EQ(ErrorCodes::UserNotFound, result);
}

TEST_F(LDAPTransformTest, tooManyResults) {
    LDAPEntityCollection results;
    results.emplace("cn=sajack" + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    results.emplace("cn=spencer" + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    runner->push(kEngineeringDN + "??one?(uid=sajack)", std::move(results));
    auto transformer =
        assertCreateOK<LDAPRewriteRule>(kEngineeringRealmRegex, kEngineeringDN + "??one?(uid={0})");
    auto result = transformer.resolve(runner, "sajack@ENGINEERING");
    ASSERT_EQ(ErrorCodes::UserDataInconsistent, result);
}

TEST_F(LDAPTransformTest, stringSubstitutionSuccess) {
    LDAPEntityCollection results;
    results.emplace("cn=sajack" + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    runner->push(kEngineeringDN + "??one?(uid=sajack)", std::move(results));
    auto transformer =
        assertCreateOK<LDAPRewriteRule>(kEngineeringRealmRegex, kEngineeringDN + "??one?(uid={0})");
    auto result = transformer.resolve(runner, "sajack@ENGINEERING");
    ASSERT_OK(result);
    ASSERT_EQ("cn=sajack" + kEngineeringDN, result.getValue());
}

TEST_F(NameMapperTest, parseEmptyConfig) {
    auto swEngine = InternalToLDAPUserNameMapper::createNameMapper(BSONArray());
    ASSERT_OK(swEngine.getStatus());
    ASSERT_NOT_OK(swEngine.getValue().transform(runner, "helloWorld"));
}

TEST_F(NameMapperTest, parseBadConfig) {
    ASSERT_NOT_OK(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSONObj())).getStatus());
    ASSERT_NOT_OK(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON("match" << 5))).getStatus());
    ASSERT_NOT_OK(InternalToLDAPUserNameMapper::createNameMapper(
                      BSON_ARRAY(BSON("match"
                                      << "cn=(.+)" + kEngineeringDN)))
                      .getStatus());
    ASSERT_NOT_OK(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON("match"
                                                                       << "cn=(.+)" + kEngineeringDN
                                                                       << "wrong"
                                                                       << "BadField")))
            .getStatus());
    ASSERT_NOT_OK(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON("match"
                                                                       << "cn=(.+)" + kEngineeringDN
                                                                       << "substitution"
                                                                       << 5)))
            .getStatus());
    ASSERT_NOT_OK(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON("match"
                                                                       << "cn=(.+)" + kEngineeringDN
                                                                       << "ldapQuery"
                                                                       << 5)))
            .getStatus());
    ASSERT_NOT_OK(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON("match"
                                                                       << "cn=(.+)" + kEngineeringDN
                                                                       << "substitution"
                                                                       << ""
                                                                       << "ldapQuery"
                                                                       << "")))
            .getStatus());
}

TEST_F(NameMapperTest, parseRule) {
    auto engineResult(
        InternalToLDAPUserNameMapper::createNameMapper(
            BSON_ARRAY(BSON("match"
                            << "cn=(.+)," + kEngineeringDN
                            << "substitution"
                            << "{0}@admin"))));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(runner, "cn=sajack," + kEngineeringDN);
    ASSERT_OK(map1);
    ASSERT_EQ("sajack@admin", map1.getValue());
}

TEST_F(NameMapperTest, parseRuleWithSingleDocument) {
    auto engineResult(
        InternalToLDAPUserNameMapper::createNameMapper(BSON("match"
                                                            << "cn=(.+)," + kEngineeringDN
                                                            << "substitution"
                                                            << "{0}@admin")));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(runner, "cn=sajack," + kEngineeringDN);
    ASSERT_OK(map1);
    ASSERT_EQ("sajack@admin", map1.getValue());
}

TEST_F(NameMapperTest, parseRuleWithEmptyMatch) {
    auto engineResult(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON("match"
                                                                       << ""
                                                                       << "substitution"
                                                                       << "{0}@admin"))));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(runner, "cn=sajack," + kEngineeringDN);
    ASSERT_NOT_OK(map1);
}

TEST_F(NameMapperTest, parseRuleWithEmptySub) {
    auto engineResult(
        InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON("match"
                                                                       << "cn=(.+)" + kEngineeringDN
                                                                       << "substitution"
                                                                       << ""))));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(runner, "cn=sajack," + kEngineeringDN);
    ASSERT_OK(map1);
    ASSERT_EQ("", map1.getValue());
}

TEST_F(NameMapperTest, parseTwoRules) {
    auto engineResult(
        InternalToLDAPUserNameMapper::createNameMapper(
            BSON_ARRAY(BSON("match"
                            << "cn=(.+)," + kEngineeringDN
                            << "substitution"
                            << "{0}@admin")
                       << BSON("match"
                               << "cn=(.+)," + kMarketingDN
                               << "substitution"
                               << "{0}@production"))));
    ASSERT_TRUE(engineResult.isOK());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};

    auto map1 = engine.transform(runner, "cn=sajack," + kEngineeringDN);
    ASSERT_OK(map1);
    ASSERT_EQ("sajack@admin", map1.getValue());

    auto map2 = engine.transform(runner, "cn=sajack," + kMarketingDN);
    ASSERT_OK(map2);
    ASSERT_EQ("sajack@production", map2.getValue());
}

TEST_F(NameMapperTest, parseLDAPRule) {
    LDAPEntityCollection results;
    results.emplace("cn=sajack," + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    runner->push(kEngineeringDN + "??one?(uid=sajack)", std::move(results));
    auto engineResult(InternalToLDAPUserNameMapper::createNameMapper(BSON_ARRAY(BSON(
        "match" << kEngineeringRealmRegex << "ldapQuery" << kEngineeringDN + "??one?(uid={0})"))));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(runner, "sajack@ENGINEERING");
    ASSERT_OK(map1);
    ASSERT_EQ("cn=sajack," + kEngineeringDN, map1.getValue());
}
}  // namespace mongo
