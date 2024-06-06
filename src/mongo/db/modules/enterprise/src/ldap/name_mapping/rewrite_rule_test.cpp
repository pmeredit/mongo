/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include <functional>
#include <memory>
#include <optional>
#include <utility>

#include "../ldap_query.h"
#include "../ldap_runner_mock.h"
#include "internal_to_ldap_user_name_mapper.h"
#include "ldap_rewrite_rule.h"
#include "mongo/base/init.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
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
    return std::make_unique<T>(assertCreateOK<T>(std::forward<Args>(args)...));
}

class LDAPTransformContext : public unittest::Test {
public:
    LDAPTransformContext()
        : runner(std::make_unique<LDAPRunnerMock>()),
          userAcquisitionStats(std::make_shared<UserAcquisitionStats>()) {}

    void assertLdapSearchOp(const LDAPOperationStats& stats, std::uint64_t expectedNumOps) {
        ASSERT_EQ(stats._searchStats.numOps, expectedNumOps);
    }

protected:
    std::unique_ptr<LDAPRunnerMock> runner;
    // These variables are used for arguments that track LDAP operations in CurOp
    SharedUserAcquisitionStats userAcquisitionStats;
    TickSourceMock<Seconds> tsSecs;
};

using RegexTransformTest = LDAPTransformContext;
using LDAPTransformTest = LDAPTransformContext;
using NameMapperTest = LDAPTransformContext;

TEST_F(RegexTransformTest, matchesAString) {
    auto regex = assertCreateOK<RegexRewriteRule>("[a-zA-Z]+", "");
    ASSERT_OK(regex.resolve(runner.get(), "helloWorld", &tsSecs, userAcquisitionStats));
}

TEST_F(RegexTransformTest, failsToMatchAString) {
    auto regex = assertCreateOK<RegexRewriteRule>("[a-zA-Z]+", "");
    ASSERT_NOT_OK(regex.resolve(runner.get(), "555", &tsSecs, userAcquisitionStats));
}

TEST_F(RegexTransformTest, substitutesAString) {
    auto regex = assertCreateOK<RegexRewriteRule>("([a-zA-Z]+)", "Got: {0}");
    auto result = regex.resolve(runner.get(), "helloWorld", &tsSecs, userAcquisitionStats);
    ASSERT_OK(result);
    ASSERT_EQ("Got: helloWorld", result.getValue());
}

TEST_F(RegexTransformTest, substitutesTwoStrings) {
    auto regex = assertCreateOK<RegexRewriteRule>("([a-zA-Z]+) ([a-zA-Z]+)", "Got: {0}. And: {1}.");
    auto result =
        regex.resolve(runner.get(), "helloWorld goodbyeWorld", &tsSecs, userAcquisitionStats);
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
    auto result = regex.resolve(runner.get(), regexInput.str(), &tsSecs, userAcquisitionStats);
    ASSERT_OK(result);
    ASSERT_EQ(regexOutput.str(), result.getValue());
}

TEST_F(RegexTransformTest, backslashesInSubstitutionDefaultPattern) {
    // The default rewrite should be a 100% passthrough without mutation.
    // Make sure backslashes don't break this assumption.
    constexpr auto kPrefix = "CN="_sd;
    constexpr auto kSuffix = ",OU=Irregular,O=MongoDB"_sd;
    auto regex = assertCreateOK<RegexRewriteRule>("(.+)", "{0}");

    {
        std::string spacePadded = str::stream() << kPrefix << "\\ Padded\\ " << kSuffix;
        auto result = regex.resolve(runner.get(), spacePadded, &tsSecs, userAcquisitionStats);
        ASSERT_OK(result);
        ASSERT_EQ(spacePadded, result.getValue());
    }

    for (const char ch : {' ', '\\', '#', '+', '<', '>', ',', ';', '"', '='}) {
        std::string DN = str::stream() << kPrefix << "[\\" << ch << "] Name" << kSuffix;
        auto result = regex.resolve(runner.get(), DN, &tsSecs, userAcquisitionStats);
        ASSERT_OK(result);
        ASSERT_EQ(DN, result.getValue());
    }
}

TEST_F(RegexTransformTest, nonCaptureGroupBraces) {
    // Test braces occuring in substitution not as part of capture group.
    const auto transform =
        [this](const std::string& pattern, std::string substitution, const std::string& input) {
            auto regex = assertCreateOK<RegexRewriteRule>(pattern, std::move(substitution));
            auto result = regex.resolve(this->runner.get(), input, &tsSecs, userAcquisitionStats);
            ASSERT_OK(result);
            return result.getValue();
        };

    // Non-numeric group placeholders are ignored.
    ASSERT_EQ(transform("(.+)", "{foo}{0}{bar}", " test "), "{foo} test {bar}");

    // Unavailable capture group placeholders are ignored.
    ASSERT_EQ(transform("(.+) (.+)", "A: {0}, B: {1}, C: {2}", "Hello World"),
              "A: Hello, B: World, C: {2}");
    // Ensure 0 capture group + non-zero substitution works
    ASSERT_EQ(transform(".+", "{0}", "test"), "{0}");

    // Unmatched braces are ignored.
    ASSERT_EQ(transform("(.+)", "}{{0}} { {0} }{", "test"), "}{test} { test }{");

    // Check for std::numeric_limits<std::size_t>::max wraparound.
    ASSERT_EQ(transform("(A)(B)(C)(D)(E)", "{4}{3}{2}{1}{0} {-1} {9223372036854775809}", "ABCDE"),
              "EDCBA {-1} {9223372036854775809}");

    // Space padding around numeric groups.
    ASSERT_EQ(transform("(.+)", "{ 0}", "test"), "{ 0}");
    ASSERT_EQ(transform("(.+)", "{0 }", "test"), "{0 }");
    ASSERT_EQ(transform("(.+)", "{ 0 }", "test"), "{ 0 }");

    // Backslashes prior to group identifier braces don't bind.
    ASSERT_EQ(transform("(.+)", "\\{0}", "test"), "\\test");
}

TEST_F(LDAPTransformTest, stringSubstitutionFailureReturnsBadStatus) {
    auto transformer =
        assertCreateOK<LDAPRewriteRule>(kEngineeringRealmRegex, "cn={0}" + kEngineeringDN);
    ASSERT_NOT_OK(
        transformer.resolve(runner.get(), "sajack@MARKETING", &tsSecs, userAcquisitionStats));
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
    auto result =
        transformer.resolve(runner.get(), "sajack@ENGINEERING", &tsSecs, userAcquisitionStats);
    ASSERT_EQ(ErrorCodes::UserNotFound, result);
    assertLdapSearchOp(userAcquisitionStats->getLdapOperationStatsSnapshot(), 1);
}

TEST_F(LDAPTransformTest, tooManyResults) {
    LDAPEntityCollection results;
    results.emplace("cn=sajack" + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    results.emplace("cn=spencer" + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    runner->push(kEngineeringDN + "??one?(uid=sajack)", std::move(results));
    auto transformer =
        assertCreateOK<LDAPRewriteRule>(kEngineeringRealmRegex, kEngineeringDN + "??one?(uid={0})");
    auto result =
        transformer.resolve(runner.get(), "sajack@ENGINEERING", &tsSecs, userAcquisitionStats);
    ASSERT_EQ(ErrorCodes::UserDataInconsistent, result);
    assertLdapSearchOp(userAcquisitionStats->getLdapOperationStatsSnapshot(), 1);
}

TEST_F(LDAPTransformTest, stringSubstitutionSuccess) {
    LDAPEntityCollection results;
    results.emplace("cn=sajack" + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    runner->push(kEngineeringDN + "??one?(uid=sajack)", std::move(results));
    auto transformer =
        assertCreateOK<LDAPRewriteRule>(kEngineeringRealmRegex, kEngineeringDN + "??one?(uid={0})");
    auto result =
        transformer.resolve(runner.get(), "sajack@ENGINEERING", &tsSecs, userAcquisitionStats);
    ASSERT_OK(result);
    ASSERT_EQ("cn=sajack" + kEngineeringDN, result.getValue());
    assertLdapSearchOp(userAcquisitionStats->getLdapOperationStatsSnapshot(), 1);
}

TEST_F(NameMapperTest, parseEmptyConfig) {
    auto swEngine = InternalToLDAPUserNameMapper::createNameMapper("");
    ASSERT_OK(swEngine.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(swEngine.getValue())};
    auto map1 = engine.transform(
        runner.get(), "cn=alice,ou=engineering,dc=example,dc=com", &tsSecs, userAcquisitionStats);
    ASSERT_OK(map1);
    ASSERT_EQ("cn=alice,ou=engineering,dc=example,dc=com", map1.getValue());
}

TEST_F(NameMapperTest, parseEmptyArrayConfig) {
    auto swEngine(InternalToLDAPUserNameMapper::createNameMapper("[]"));
    ASSERT_OK(swEngine.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(swEngine.getValue())};
    auto map1 = engine.transform(
        runner.get(), "cn=alice,ou=engineering,dc=example,dc=com", &tsSecs, userAcquisitionStats);
    ASSERT_OK(map1);
    ASSERT_EQ("cn=alice,ou=engineering,dc=example,dc=com", map1.getValue());
}

TEST_F(NameMapperTest, parseBadConfig) {
    ASSERT_NOT_OK(InternalToLDAPUserNameMapper::createNameMapper("{match:5}").getStatus());
    ASSERT_NOT_OK(InternalToLDAPUserNameMapper::createNameMapper("{match:cn=(.+)" + kEngineeringDN)
                      .getStatus());
    ASSERT_NOT_OK(InternalToLDAPUserNameMapper::createNameMapper("{match:cn=(.+)" + kEngineeringDN +
                                                                 ",wrong:BadField}")
                      .getStatus());
    ASSERT_NOT_OK(InternalToLDAPUserNameMapper::createNameMapper("{match:cn=(.+)" + kEngineeringDN +
                                                                 ",substitution:5}")
                      .getStatus());
    ASSERT_NOT_OK(InternalToLDAPUserNameMapper::createNameMapper("{match:cn=(.+)" + kEngineeringDN +
                                                                 ",ldapQuery:5}")
                      .getStatus());
    ASSERT_NOT_OK(InternalToLDAPUserNameMapper::createNameMapper("{match:cn=(.+)" + kEngineeringDN +
                                                                 ",substitution:'',ldapQuery:''}")
                      .getStatus());
}

TEST_F(NameMapperTest, parseRule) {
    auto engineResult(InternalToLDAPUserNameMapper::createNameMapper(
        "[{match:'cn=(.+)," + kEngineeringDN + "',substitution:'{0}@admin'}]"));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(
        runner.get(), "cn=sajack," + kEngineeringDN, &tsSecs, userAcquisitionStats);
    ASSERT_OK(map1);
    ASSERT_EQ("sajack@admin", map1.getValue());
}

TEST_F(NameMapperTest, parseRuleWithSingleDocument) {
    auto engineResult(InternalToLDAPUserNameMapper::createNameMapper(
        "{match:'cn=(.+)," + kEngineeringDN + "',substitution:'{0}@admin'}"));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(
        runner.get(), "cn=sajack," + kEngineeringDN, &tsSecs, userAcquisitionStats);
    ASSERT_OK(map1);
    ASSERT_EQ("sajack@admin", map1.getValue());
}

TEST_F(NameMapperTest, parseRuleWithEmptyMatch) {
    auto engineResult(
        InternalToLDAPUserNameMapper::createNameMapper("{match:'',substitution:'{0}@admin'}"));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(
        runner.get(), "cn=sajack," + kEngineeringDN, &tsSecs, userAcquisitionStats);
    ASSERT_NOT_OK(map1);
}

TEST_F(NameMapperTest, parseRuleWithEmptySub) {
    auto engineResult(InternalToLDAPUserNameMapper::createNameMapper(
        "{match:'cn=(.+)" + kEngineeringDN + "',substitution:''}"));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(
        runner.get(), "cn=sajack," + kEngineeringDN, &tsSecs, userAcquisitionStats);
    ASSERT_OK(map1);
    ASSERT_EQ("", map1.getValue());
}

TEST_F(NameMapperTest, parseTwoRules) {
    auto engineResult(InternalToLDAPUserNameMapper::createNameMapper(
        "[{match:'cn=(.+)," + kEngineeringDN + "',substitution:'{0}@admin'},{match:'cn=(.+)," +
        kMarketingDN + "',substitution:'{0}@production'}]"));
    ASSERT_TRUE(engineResult.isOK());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};

    auto map1 = engine.transform(
        runner.get(), "cn=sajack," + kEngineeringDN, &tsSecs, userAcquisitionStats);
    ASSERT_OK(map1);
    ASSERT_EQ("sajack@admin", map1.getValue());

    auto map2 =
        engine.transform(runner.get(), "cn=sajack," + kMarketingDN, &tsSecs, userAcquisitionStats);
    ASSERT_OK(map2);
    ASSERT_EQ("sajack@production", map2.getValue());
}

TEST_F(NameMapperTest, parseLDAPRule) {
    LDAPEntityCollection results;
    results.emplace("cn=sajack," + kEngineeringDN, LDAPAttributeKeyValuesMap{});
    runner->push(kEngineeringDN + "??one?(uid=sajack)", std::move(results));
    auto engineResult(InternalToLDAPUserNameMapper::createNameMapper(
        "{match:'" + kEngineeringRealmRegex + "',ldapQuery:'" + kEngineeringDN +
        "??one?(uid={0})'}"));
    ASSERT_OK(engineResult.getStatus());
    InternalToLDAPUserNameMapper engine{std::move(engineResult.getValue())};
    auto map1 = engine.transform(runner.get(), "sajack@ENGINEERING", &tsSecs, userAcquisitionStats);
    ASSERT_OK(map1);
    ASSERT_EQ("cn=sajack," + kEngineeringDN, map1.getValue());
    assertLdapSearchOp(userAcquisitionStats->getLdapOperationStatsSnapshot(), 1);
}
}  // namespace mongo
