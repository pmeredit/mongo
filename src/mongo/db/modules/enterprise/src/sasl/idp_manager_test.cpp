/**
 * Copyright (C) 2022-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"

#include "mongo/crypto/jwks_fetcher_mock.h"

#include "sasl/idp_manager.h"

namespace mongo::auth {
namespace {
using namespace fmt::literals;

constexpr auto kIssuer1 = "https://test.kernel.mongodb.com/IDPManager1"_sd;
constexpr auto kIssuer2 = "https://test.kernel.mongodb.com/IDPManager2"_sd;
constexpr auto kIssuer3 = "https://test.kernel.mongodb.com/IDPManager3"_sd;
constexpr auto kAudience1 = "jwt@kernel.mongodb.com"_sd;
constexpr auto kAudience2 = "jwt@kernel.10gen.com"_sd;

class MockJWKSFetcherFactory : public JWKSFetcherFactory {
public:
    MockJWKSFetcherFactory() : _clock(std::make_unique<ClockSourceMock>()) {}

    BSONObj getTestJWKSet(bool includeKnownKeyTypes = true,
                          bool includeUnknownKeyTypes = false) const {
        BSONObjBuilder set;
        BSONArrayBuilder keys(set.subarrayStart("keys"_sd));

        if (includeKnownKeyTypes) {
            BSONObjBuilder key(keys.subobjStart());
            key.append("kty", "RSA");
            key.append("kid", "custom-key-1");
            key.append("e", "AQAB");
            key.append(
                "n",
                "ALtUlNS31SzxwqMzMR9jKOJYDhHj8zZtLUYHi3s1en3wLdILp1Uy8O6Jy0Z66tPyM1u8lke0JK5gS-"
                "40yhJ-"
                "bvqioW8CnwbLSLPmzGNmZKdfIJ08Si8aEtrRXMxpDyz4Is7JLnpjIIUZ4lmqC3MnoZHd6qhhJb1v1Qy-"
                "QGlk4NJy1ZI0aPc_uNEUM7lWhPAJABZsWc6MN8flSWCnY8pJCdIk_cAktA0U17tuvVduuFX_"
                "94763nWYikZIMJS_cTQMMVxYNMf1xcNNOVFlUSJHYHClk46QT9nT8FWeFlgvvWhlXfhsp9aNAi3pX-"
                "KxIxqF2wABIAKnhlMa3CJW41323Js");
            key.doneFast();
        }
        if (includeKnownKeyTypes) {
            BSONObjBuilder key(keys.subobjStart());
            key.append("kty", "RSA");
            key.append("kid", "custom-key-2");
            key.append("e", "AQAB");
            key.append(
                "n",
                "4Amo26gLJITvt62AXI7z224KfvfQjwpyREjtpA2DU2mN7pnlz-"
                "ZDu0sygwkhGcAkRPVbzpEiliXtVo2dYN4vMKLSd5BVBXhtB41bZ6OUxni48uP5txm7w8BUWv8MxzPkzyW_"
                "3dd8rOfzECdLCF5G3aA4u_XRu2ODUSAMcrxXngnNtAuC-"
                "OdqgYmvZfgFwqbU0VKNR4bbkhSrw6p9Tct6CUW04Ml4HMacZUovJKXRvNqnHcx3sy4PtVe3CyKlbb4KhBt"
                "kj1U"
                "U_"
                "cwiosz8uboBbchp7wsATieGVF8x3BUtf0ry94BGYXKbCGY_Mq-TSxcM_3afZiJA1COVZWN7d4GTEw");
            key.doneFast();
        }
        if (includeUnknownKeyTypes) {
            BSONObjBuilder key(keys.subobjStart());
            key.append("kty", "Foo");
            key.append("kid", "unknown-key-1");
            key.append("field1", "AQAB");
            key.doneFast();
        }
        keys.doneFast();
        return set.obj();
    }

    std::unique_ptr<crypto::JWKSFetcher> makeJWKSFetcher(StringData issuer) const final {
        auto fetcher = std::make_unique<crypto::MockJWKSFetcher>(
            _clock.get(), getTestJWKSet(_includeKnownKeyTypes, _includeUnknownKeyTypes));
        if (_shouldFail) {
            fetcher->setShouldFail(_shouldFail);
        }

        return fetcher;
    }

    void setShouldFail(bool shouldFail) {
        _shouldFail = shouldFail;
    }
    void setIncludeUnknownKeyTypes(bool include) {
        _includeUnknownKeyTypes = include;
    }
    void setIncludeKnownKeyTypes(bool include) {
        _includeKnownKeyTypes = include;
    }

    ClockSourceMock* getClock() {
        return _clock.get();
    }

private:
    bool _shouldFail{false};
    bool _includeUnknownKeyTypes{false};
    bool _includeKnownKeyTypes{true};
    std::unique_ptr<ClockSourceMock> _clock;
};

TEST(IDPManager, singleIDP) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration idpc;
    idpc.setIssuer(kIssuer1);
    idpc.setAudience(kAudience1);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, {std::move(idpc)});

    // Get Issuer by name.
    ASSERT_OK(idpm.getIDP(kIssuer1, kAudience1));
    ASSERT_NOT_OK(idpm.getIDP(kIssuer2, kAudience1));
    ASSERT_NOT_OK(idpm.getIDP(kIssuer1, kAudience2));

    // Get one and only configured issuer.
    ASSERT_OK(idpm.selectIDP(boost::none));
}

TEST(IDPManager, multipleIDPs) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration issuer1;
    issuer1.setIssuer(kIssuer1);
    issuer1.setAudience(kAudience1);
    issuer1.setMatchPattern("@mongodb.com$"_sd);

    IDPConfiguration issuer2;
    issuer2.setIssuer(kIssuer2);
    issuer2.setAudience(kAudience2);
    issuer2.setMatchPattern("@10gen.com$"_sd);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, {std::move(issuer1), std::move(issuer2)});

    // Get Issuer by name.
    auto swIssuer1 = idpm.getIDP(kIssuer1, kAudience1);
    ASSERT_OK(swIssuer1.getStatus());

    auto swIssuer2 = idpm.getIDP(kIssuer2, kAudience2);
    ASSERT_OK(swIssuer2.getStatus());

    ASSERT_NOT_OK(idpm.getIDP(kIssuer1, kAudience2));
    ASSERT_NOT_OK(idpm.getIDP(kIssuer2, kAudience1));

    // Get Issuer by principal name hint.
    auto swHinted1 = idpm.selectIDP("user1@mongodb.com"_sd);
    ASSERT_OK(swHinted1.getStatus());
    ASSERT_EQ(swHinted1.getValue()->getConfig().getIssuer(), kIssuer1);

    auto swHinted2 = idpm.selectIDP("user1@10gen.com"_sd);
    ASSERT_OK(swHinted2.getStatus());
    ASSERT_EQ(swHinted2.getValue()->getConfig().getIssuer(), kIssuer2);

    auto swHinted3 = idpm.selectIDP("user1@atlas.mongodb.com"_sd);
    ASSERT_NOT_OK(swHinted3.getStatus());
}

TEST(IDPManager, multipleIDPsWithSameIssuer) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration cfg1, cfg2, cfg3;
    cfg1.setIssuer(kIssuer1);
    cfg1.setAudience(kAudience1);
    cfg1.setMatchPattern("@mongodb.com$"_sd);

    cfg2.setIssuer(kIssuer1);
    cfg2.setAudience(kAudience2);
    cfg2.setMatchPattern("@10gen.com$"_sd);

    cfg3.setIssuer(kIssuer2);
    cfg3.setAudience(kAudience2);
    cfg3.setMatchPattern("@example.com$"_sd);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, {std::move(cfg1), std::move(cfg2), std::move(cfg3)});
    ASSERT_EQ(3, idpm.size());

    auto idp1 = uassertStatusOK(idpm.getIDP(kIssuer1, kAudience1));
    auto idp2 = uassertStatusOK(idpm.getIDP(kIssuer1, kAudience2));
    auto idp3 = uassertStatusOK(idpm.getIDP(kIssuer2, kAudience2));

    ASSERT_NOT_OK(idpm.getIDP(kIssuer1, "foo"_sd));
    ASSERT_NOT_OK(idpm.getIDP(kIssuer2, kAudience1));
    ASSERT_NOT_OK(idpm.getIDP(kIssuer3, kAudience2));

    // IDPs with same issuer must have the same key refresher
    ASSERT_EQ(idp1->getKeyRefresher(), idp2->getKeyRefresher());
    ASSERT_NE(idp1->getKeyRefresher(), idp3->getKeyRefresher());

    // Get Issuer by principal name hint.
    auto hinted1 = uassertStatusOK(idpm.selectIDP("user1@mongodb.com"_sd));
    ASSERT_EQ(hinted1, idp1);

    auto hinted2 = uassertStatusOK(idpm.selectIDP("user1@10gen.com"_sd));
    ASSERT_EQ(hinted2, idp2);

    auto hinted3 = uassertStatusOK(idpm.selectIDP("user1@example.com"_sd));
    ASSERT_EQ(hinted3, idp3);

    ASSERT_NOT_OK(idpm.selectIDP("user1@atlas.mongodb.com"_sd).getStatus());
}

TEST(IDPManager, updateConfigurationsResetsIdentityProviders) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    auto configs = [] {
        IDPConfiguration cfg1, cfg2, cfg3;
        cfg1.setIssuer(kIssuer1);
        cfg1.setAudience(kAudience1);
        cfg1.setMatchPattern("@mongodb.com$"_sd);

        cfg2.setIssuer(kIssuer1);
        cfg2.setAudience(kAudience2);
        cfg2.setMatchPattern("@10gen.com$"_sd);

        cfg3.setIssuer(kIssuer2);
        cfg3.setAudience(kAudience2);
        cfg3.setMatchPattern("@example.com$"_sd);
        return std::vector<IDPConfiguration>{std::move(cfg1), std::move(cfg2), std::move(cfg3)};
    }();

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    // Initial configs update
    idpm.updateConfigurations(nullptr, configs);
    ASSERT_EQ(3, idpm.size());

    auto idp1 = uassertStatusOK(idpm.getIDP(kIssuer1, kAudience1));
    auto idp2 = uassertStatusOK(idpm.getIDP(kIssuer1, kAudience2));
    auto idp3 = uassertStatusOK(idpm.getIDP(kIssuer2, kAudience2));

    // Update configurations
    idpm.updateConfigurations(nullptr, configs);
    ASSERT_EQ(3, idpm.size());

    auto newIdp1 = uassertStatusOK(idpm.getIDP(kIssuer1, kAudience1));
    ASSERT_NE(newIdp1, idp1);
    ASSERT_NE(newIdp1->getKeyRefresher(), idp1->getKeyRefresher());

    auto newIdp2 = uassertStatusOK(idpm.getIDP(kIssuer1, kAudience2));
    ASSERT_NE(newIdp2, idp2);
    ASSERT_NE(newIdp2->getKeyRefresher(), idp2->getKeyRefresher());

    auto newIdp3 = uassertStatusOK(idpm.getIDP(kIssuer2, kAudience2));
    ASSERT_NE(newIdp3, idp3);
    ASSERT_NE(newIdp3->getKeyRefresher(), idp3->getKeyRefresher());

    ASSERT_EQ(newIdp1->getKeyRefresher(), newIdp2->getKeyRefresher());
    ASSERT_NE(newIdp3->getKeyRefresher(), newIdp2->getKeyRefresher());
}

TEST(IDPManager, unsetHintWithMultipleMatchPatternsFails) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration issuer1;
    issuer1.setIssuer(kIssuer1);
    issuer1.setMatchPattern("@mongodb.com$"_sd);

    IDPConfiguration issuer2;
    issuer2.setIssuer(kIssuer2);
    issuer2.setMatchPattern("@10gen.com$"_sd);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, {std::move(issuer1), std::move(issuer2)});

    auto swHinted = idpm.selectIDP(boost::none);
    ASSERT_NOT_OK(swHinted.getStatus());
}

TEST(IDPManager, unsetHintWithMultipleIdPs) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration issuer1;
    issuer1.setIssuer(kIssuer1);
    issuer1.setSupportsHumanFlows(false);

    IDPConfiguration issuer2;
    issuer2.setIssuer(kIssuer2);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, {std::move(issuer1), std::move(issuer2)});

    // With no hint set, default to the sole human flow
    auto swHinted = idpm.selectIDP(boost::none);
    ASSERT_OK(swHinted.getStatus());
    ASSERT_EQ(swHinted.getValue()->getIssuer(), kIssuer2);
}

TEST(IDPManager, firstMatchingIdPWins) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    auto configs = [] {
        IDPConfiguration cfg1, cfg2, cfg3;
        cfg1.setIssuer(kIssuer1);
        cfg1.setAudience(kAudience1);
        cfg1.setMatchPattern("ongo.*com$"_sd);

        cfg2.setIssuer(kIssuer2);
        cfg2.setAudience(kAudience2);
        cfg2.setMatchPattern("@mongodb.com$"_sd);

        cfg3.setIssuer(kIssuer1);
        cfg3.setAudience(kAudience2);
        cfg3.setMatchPattern("go.*com$"_sd);
        return std::vector<IDPConfiguration>{std::move(cfg1), std::move(cfg2), std::move(cfg3)};
    }();

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, configs);

    auto hinted = uassertStatusOK(idpm.selectIDP("foo@mongodb.com"_sd));
    ASSERT_EQ(hinted->getIssuer(), kIssuer1);
    ASSERT_EQ(hinted->getAudience(), kAudience1);

    hinted = uassertStatusOK(idpm.selectIDP("foo@bongodb.com"_sd));
    ASSERT_EQ(hinted->getIssuer(), kIssuer1);
    ASSERT_EQ(hinted->getAudience(), kAudience1);

    hinted = uassertStatusOK(idpm.selectIDP("foo@bingodb.com"_sd));
    ASSERT_EQ(hinted->getIssuer(), kIssuer1);
    ASSERT_EQ(hinted->getAudience(), kAudience2);
}

TEST(IDPJWKSRefresher, refreshIDPKeys) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration idpConfig;
    idpConfig.setIssuer(kIssuer1);
    idpConfig.setMatchPattern("@mongodb.com$"_sd);

    // Set the JWKSetFetcherFactory to fail initially, which should result in no keys loaded to the
    // IdentityProvider.
    auto uniqueFetcherFactory = std::make_unique<MockJWKSFetcherFactory>();
    auto* fetcherFactory = uniqueFetcherFactory.get();
    fetcherFactory->setShouldFail(true);
    auto refresher = std::make_unique<IDPJWKSRefresher>(*fetcherFactory, idpConfig);

    // Assert that the refresher initially has no keys due to the failed fetch.
    BSONObjBuilder initialKeySetBob;
    refresher->serializeJWKSet(&initialKeySetBob);
    auto initialKeySet = initialKeySetBob.obj();

    ASSERT_BSONOBJ_EQ(initialKeySet, BSON("keys" << BSONArray()));

    // Now, allow the fetcher to start succeeding. The successful refresh should result in the keys
    // being properly loaded into the IdentityProvider's JWKManager.
    fetcherFactory->setShouldFail(false);
    ASSERT_OK(refresher->refreshKeys(*fetcherFactory, IDPJWKSRefresher::RefreshOption::kNow));

    BSONObjBuilder successfulRefreshKeySetBob;
    refresher->serializeJWKSet(&successfulRefreshKeySetBob);
    auto successfulRefreshKeySet = successfulRefreshKeySetBob.obj();
    auto testJWKSet = fetcherFactory->getTestJWKSet();

    ASSERT_BSONOBJ_EQ(successfulRefreshKeySet, testJWKSet);

    // Simulate a failed refresh. The keys should remain unchanged.
    fetcherFactory->setShouldFail(true);
    ASSERT_NOT_OK(refresher->refreshKeys(*fetcherFactory, IDPJWKSRefresher::RefreshOption::kNow));

    BSONObjBuilder failedRefreshKeySetBob;
    refresher->serializeJWKSet(&failedRefreshKeySetBob);
    auto failedRefreshKeySet = failedRefreshKeySetBob.obj();

    ASSERT_BSONOBJ_EQ(failedRefreshKeySet, testJWKSet);
}

TEST(IDPManager, serializeJWKSets) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration cfg1, cfg2, cfg3;
    cfg1.setIssuer(kIssuer1);
    cfg1.setAudience(kAudience1);

    cfg2.setIssuer(kIssuer1);
    cfg2.setAudience(kAudience2);

    cfg3.setIssuer(kIssuer2);
    cfg3.setAudience(kAudience2);

    std::vector<IDPConfiguration> issuer1Configs = {cfg1, cfg2};
    std::vector<IDPConfiguration> issuer2Configs = {cfg3};
    std::vector<IDPConfiguration> allConfigs = {cfg1, cfg2, cfg3};

    auto uniqueFetcherFactory = std::make_unique<MockJWKSFetcherFactory>();
    auto fetcherFactory = uniqueFetcherFactory.get();
    IDPManager idpManager(std::move(uniqueFetcherFactory));

    // Assert that the IDP manager initially has no keys
    {
        BSONObjBuilder bob;
        idpManager.serializeJWKSets(&bob, boost::none);
        ASSERT(bob.obj().isEmpty());
    }

    // Load configs for issuer1 only
    idpManager.updateConfigurations(nullptr, issuer1Configs);

    // The IDP manager should only contain one JWKS for issuer1
    {
        BSONObjBuilder bob;
        idpManager.serializeJWKSets(&bob, boost::none);
        ASSERT_BSONOBJ_EQ(bob.obj(), BSON(kIssuer1 << fetcherFactory->getTestJWKSet()));
    }

    // Load configs for issuer2 only
    idpManager.updateConfigurations(nullptr, issuer2Configs);

    // The IDP manager should only contain one JWKS for issuer2
    {
        BSONObjBuilder bob;
        idpManager.serializeJWKSets(&bob, boost::none);
        ASSERT_BSONOBJ_EQ(bob.obj(), BSON(kIssuer2 << fetcherFactory->getTestJWKSet()));
    }

    // Load all configs
    idpManager.updateConfigurations(nullptr, allConfigs);

    // The IDP manager should contain one JWKS for each of issuer1 and issuer2
    {
        BSONObjBuilder issuer1bob, issuer2bob;
        idpManager.serializeJWKSets(&issuer1bob, std::set<StringData>{kIssuer1});
        idpManager.serializeJWKSets(&issuer2bob, std::set<StringData>{kIssuer2});
        ASSERT_BSONOBJ_EQ(issuer1bob.obj(), BSON(kIssuer1 << fetcherFactory->getTestJWKSet()));
        ASSERT_BSONOBJ_EQ(issuer2bob.obj(), BSON(kIssuer2 << fetcherFactory->getTestJWKSet()));
    }
}

TEST(IDPManager, serializeConfig) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration cfg1, cfg2, cfg3;
    cfg1.setIssuer(kIssuer1);
    cfg1.setAudience(kAudience1);
    cfg1.setAuthNamePrefix("foo"_sd);

    cfg2.setIssuer(kIssuer1);
    cfg2.setAudience(kAudience2);
    cfg2.setAuthNamePrefix("foo"_sd);

    cfg3.setIssuer(kIssuer2);
    cfg3.setAudience(kAudience2);
    cfg3.setAuthNamePrefix("foo"_sd);

    std::vector<IDPConfiguration> issuer1Configs = {cfg1, cfg2};
    std::vector<IDPConfiguration> issuer2Configs = {cfg3};
    std::vector<IDPConfiguration> allConfigs = {cfg1, cfg2, cfg3};

    auto uniqueFetcherFactory = std::make_unique<MockJWKSFetcherFactory>();
    IDPManager idpManager(std::move(uniqueFetcherFactory));

    // Assert that the IDP manager initially has no configs
    {
        BSONArrayBuilder bab;
        idpManager.serializeConfig(&bab);
        ASSERT(bab.obj().isEmpty());
    }

    // Load configs for issuer1 only
    idpManager.updateConfigurations(nullptr, issuer1Configs);

    // The IDP manager should only contain issuer1 configs
    {
        BSONArrayBuilder bab;
        idpManager.serializeConfig(&bab);
        ASSERT_EQ(bab.arrSize(), issuer1Configs.size());
        for (auto elt : bab.obj()) {
            ASSERT(elt.isABSONObj());
            ASSERT_EQ(elt.Obj().getStringField("issuer"_sd), kIssuer1);
        }
    }

    // Load configs for issuer2 only
    idpManager.updateConfigurations(nullptr, issuer2Configs);

    // The IDP manager should only contain issuer2 configs
    {
        BSONArrayBuilder bab;
        idpManager.serializeConfig(&bab);
        ASSERT_EQ(bab.arrSize(), issuer2Configs.size());
        for (auto elt : bab.obj()) {
            ASSERT(elt.isABSONObj());
            ASSERT_EQ(elt.Obj().getStringField("issuer"_sd), kIssuer2);
        }
    }

    // Load all configs
    idpManager.updateConfigurations(nullptr, allConfigs);

    // The IDP manager should contain all configs
    {
        BSONArrayBuilder bab;
        idpManager.serializeConfig(&bab);
        ASSERT_EQ(bab.arrSize(), allConfigs.size());
    }
}

TEST(IDPManager, getNextRefreshTime) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration cfg1, cfg2, cfg3;
    cfg1.setIssuer(kIssuer1);
    cfg1.setAudience(kAudience1);
    cfg1.setJWKSPollSecs(Seconds(10));

    cfg2.setIssuer(kIssuer1);
    cfg2.setAudience(kAudience2);
    cfg2.setJWKSPollSecs(Seconds(10));

    cfg3.setIssuer(kIssuer2);
    cfg3.setAudience(kAudience2);
    cfg3.setJWKSPollSecs(Seconds(20));

    std::vector<IDPConfiguration> configs = {cfg1, cfg2, cfg3};

    IDPManager idpManager(std::make_unique<MockJWKSFetcherFactory>());

    auto previousRefreshTime = idpManager.getNextRefreshTime();
    ASSERT(previousRefreshTime == Date_t{stdx::chrono::system_clock::time_point::max()});

    // Assert initial fetch updates refresh time, and sets it close to the expected time
    auto expectedNextTime = Date_t::now() + cfg1.getJWKSPollSecs();
    idpManager.updateConfigurations(nullptr, configs);
    ASSERT_NE(idpManager.getNextRefreshTime(), previousRefreshTime);
    auto elapsed = idpManager.getNextRefreshTime() - expectedNextTime;
    ASSERT(elapsed < Seconds(1));

    previousRefreshTime = idpManager.getNextRefreshTime();

    // Wait for a bit
    sleep(1);

    // Assert a force-refresh of all IDPs also advances the next refresh time
    expectedNextTime = Date_t::now() + cfg1.getJWKSPollSecs();
    ASSERT_OK(idpManager.refreshAllIDPs(nullptr, IDPJWKSRefresher::RefreshOption::kNow));
    ASSERT_NE(idpManager.getNextRefreshTime(), previousRefreshTime);
    elapsed = idpManager.getNextRefreshTime() - expectedNextTime;
    ASSERT(elapsed < Seconds(1));

    previousRefreshTime = idpManager.getNextRefreshTime();

    // Assert selective force-refresh of the IDP with a larger poll secs does not
    // update the next refresh time.
    ASSERT_OK(idpManager.refreshIDPs(nullptr, {kIssuer2}, IDPJWKSRefresher::RefreshOption::kNow));
    ASSERT_EQ(idpManager.getNextRefreshTime(), previousRefreshTime);

    // Assert if-due refresh does not update the next refresh time
    ASSERT_OK(idpManager.refreshAllIDPs(nullptr));
    ASSERT_EQ(idpManager.getNextRefreshTime(), previousRefreshTime);
}

TEST(IDPJWKSRefresher, unknownKeyTypesDisregarded) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    IDPConfiguration idpConfig;
    idpConfig.setIssuer(kIssuer1);
    idpConfig.setMatchPattern("@mongodb.com$"_sd);

    auto getLoadedKeySet = [](IDPJWKSRefresher* refresher) {
        BSONObjBuilder keySetBob;
        refresher->serializeJWKSet(&keySetBob);
        return keySetBob.obj();
    };

    auto uniqueFetcherFactory = std::make_unique<MockJWKSFetcherFactory>();
    auto* fetcherFactory = uniqueFetcherFactory.get();

    // Set the JWKS to include both known and unknown key types
    fetcherFactory->setIncludeKnownKeyTypes(true);
    fetcherFactory->setIncludeUnknownKeyTypes(true);
    auto refresher = std::make_unique<IDPJWKSRefresher>(*fetcherFactory, idpConfig);

    // Assert that only known key types are loaded
    auto keySet = getLoadedKeySet(refresher.get());
    auto initialKeySet =
        fetcherFactory->getTestJWKSet(true /* include known */, false /* exclude unknown*/);

    ASSERT_BSONOBJ_EQ(keySet, initialKeySet);

    // Set the JWKS to include only unknown key types
    fetcherFactory->setIncludeKnownKeyTypes(false);

    // Loaded key set should be empty after refresh
    ASSERT_OK(refresher->refreshKeys(*fetcherFactory, IDPJWKSRefresher::RefreshOption::kNow));
    keySet = getLoadedKeySet(refresher.get());
    ASSERT_BSONOBJ_EQ(keySet, BSON("keys" << BSONArray()));

    // Set the JWKS to include only known key types
    fetcherFactory->setIncludeKnownKeyTypes(true);
    fetcherFactory->setIncludeUnknownKeyTypes(false);

    // Loaded key set should be back to initial state after refresh
    ASSERT_OK(refresher->refreshKeys(*fetcherFactory, IDPJWKSRefresher::RefreshOption::kNow));
    keySet = getLoadedKeySet(refresher.get());
    ASSERT_BSONOBJ_EQ(keySet, initialKeySet);

    // Set the JWKS to be the empty set
    fetcherFactory->setIncludeKnownKeyTypes(false);

    // Loaded key set should be back to empty
    ASSERT_OK(refresher->refreshKeys(*fetcherFactory, IDPJWKSRefresher::RefreshOption::kNow));
    keySet = getLoadedKeySet(refresher.get());
    ASSERT_BSONOBJ_EQ(keySet, BSON("keys" << BSONArray()));
}

BSONObjBuilder makeIssuerBSONObjBuilder(StringData issuer, StringData audience, StringData prefix) {
    BSONObjBuilder builder;
    builder.append("issuer", issuer);
    builder.append("audience", audience);
    builder.append("authNamePrefix", prefix);
    return builder;
}

BSONObjBuilder makeHumanIssuerBSONObjBuilder(StringData issuer,
                                             StringData audience,
                                             StringData prefix,
                                             bool matcher) {
    auto builder = makeIssuerBSONObjBuilder(issuer, audience, prefix);
    if (matcher) {
        builder.append("matchPattern", ".*@mongodb.com");
    }
    builder.append("authorizationClaim", "groups");
    builder.append("clientId", "foo");
    builder.append("supportsHumanFlows", true);
    return builder;
}

BSONObj makeHumanIssuerBSON(StringData issuer, bool matcher, StringData audience = "mongo"_sd) {
    return makeHumanIssuerBSONObjBuilder(issuer, audience, "prefix"_sd, matcher).obj();
}

BSONObj makeMachineIssuerBSON(StringData issuer) {
    BSONObjBuilder builder = makeIssuerBSONObjBuilder(issuer, "mongo"_sd, "prefix"_sd);
    builder.append("authorizationClaim", "groups");
    builder.append("supportsHumanFlows", false);
    return builder.obj();
}

TEST(IDPManager, oneHumanIdPWithoutMatchers) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration = BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, twoIdPsWithSameIssuerAndAudienceFails) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true) << makeHumanIssuerBSON(kIssuer1, true));
    ASSERT_THROWS_WITH_CHECK(IDPManager::parseConfigFromBSONObj(configuration),
                             DBException,
                             ([&](const DBException& ex) {
                                 ASSERT(std::string(ex.what()).starts_with(
                                     "Duplicate configuration for issuer-audience pair"));
                             }));
}

TEST(IDPManager, twoIdPsWithSameIssuerAndDifferentAudience) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    auto configuration = BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true, "mongo")
                                    << makeHumanIssuerBSON(kIssuer1, true, "mango"));
    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, twoIdPsWithSameIssuerAndDifferentJWKSPollSecs) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);

    auto cfg1 = makeHumanIssuerBSONObjBuilder(kIssuer1, "mongo", "prefix1", true);
    cfg1.append("JWKSPollSecs", 60);
    auto cfg2 = makeHumanIssuerBSONObjBuilder(kIssuer1, "mango", "prefix2", true);
    cfg2.append("JWKSPollSecs", 61);

    auto configuration = BSON_ARRAY(cfg1.obj() << cfg2.obj());
    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration),
        DBException,
        "IDP configurations with issuer '{}' must have the same JWKSPollSecs value"_format(
            kIssuer1));
}

TEST(IDPManager, twoHumanIdPsWithoutMatchersFail) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false) << makeHumanIssuerBSON(kIssuer2, false));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, twoHumanIdPsWithOneMatcherFails) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true) << makeHumanIssuerBSON(kIssuer2, false));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, oneMachineIdP) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration = BSON_ARRAY(makeMachineIssuerBSON(kIssuer1));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, twoMachineIdPs) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeMachineIssuerBSON(kIssuer1) << makeMachineIssuerBSON(kIssuer2));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, oneHumanOneMachineIdPsWithoutMatchers) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false) << makeMachineIssuerBSON(kIssuer2));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, twoHumanOneMachineIdPsWithoutMatchersFail) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false)
                   << makeHumanIssuerBSON(kIssuer2, false) << makeMachineIssuerBSON(kIssuer3));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, twoHumanOneMachineIdPsWithOneMatchersFail) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true)
                   << makeHumanIssuerBSON(kIssuer2, false) << makeMachineIssuerBSON(kIssuer3));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, twoHumanOneMachineIdPsWithMatchers) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true)
                   << makeHumanIssuerBSON(kIssuer2, true) << makeMachineIssuerBSON(kIssuer3));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, matchersMustBeFirst) {
    RAIIServerParameterControllerForTest quiesceController("JWKSMinimumQuiescePeriodSecs", 0);
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeMachineIssuerBSON(kIssuer1) << makeHumanIssuerBSON(kIssuer2, true));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration),
        DBException,
        "All IdPs without matchPatterns must be listed after those with matchPatterns");
}

}  // namespace
}  // namespace mongo::auth
