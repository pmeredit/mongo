/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/idl/server_parameter_test_util.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/unittest.h"

#include "mongo/crypto/jwks_fetcher_mock.h"

#include "sasl/idp_manager.h"

namespace mongo::auth {
namespace {

constexpr auto kIssuer1 = "https://test.kernel.mongodb.com/IDPManager1"_sd;
constexpr auto kIssuer2 = "https://test.kernel.mongodb.com/IDPManager2"_sd;
constexpr auto kIssuer3 = "https://test.kernel.mongodb.com/IDPManager3"_sd;


class MockJWKSFetcherFactory : public JWKSFetcherFactory {
public:
    BSONObj getTestJWKSet() const {
        BSONObjBuilder set;
        BSONArrayBuilder keys(set.subarrayStart("keys"_sd));

        {
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
        {
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

        keys.doneFast();
        return set.obj();
    }

    std::unique_ptr<crypto::JWKSFetcher> makeJWKSFetcher(StringData issuer) const final {
        auto fetcher = std::make_unique<crypto::MockJWKSFetcher>(getTestJWKSet());
        if (_shouldFail) {
            fetcher->setShouldFail(_shouldFail);
        }

        return fetcher;
    }

    void setShouldFail(bool shouldFail) {
        _shouldFail = shouldFail;
    }

private:
    bool _shouldFail{false};
};

TEST(IDPManager, singleIDP) {
    IDPConfiguration idpc;
    idpc.setIssuer(kIssuer1);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, {std::move(idpc)});

    // Get Issuer by name.
    ASSERT_OK(idpm.getIDP(kIssuer1));
    ASSERT_NOT_OK(idpm.getIDP(kIssuer2));

    // Get one and only configured issuer.
    ASSERT_OK(idpm.selectIDP(boost::none));
}

TEST(IDPManager, multipleIDPs) {
    IDPConfiguration issuer1;
    issuer1.setIssuer(kIssuer1);
    issuer1.setMatchPattern("@mongodb.com$"_sd);

    IDPConfiguration issuer2;
    issuer2.setIssuer(kIssuer2);
    issuer2.setMatchPattern("@10gen.com$"_sd);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    idpm.updateConfigurations(nullptr, {std::move(issuer1), std::move(issuer2)});

    // Get Issuer by name.
    auto swIssuer1 = idpm.getIDP(kIssuer1);
    ASSERT_OK(swIssuer1.getStatus());

    auto swIssuer2 = idpm.getIDP(kIssuer2);
    ASSERT_OK(swIssuer2.getStatus());

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

TEST(IDPManager, unsetHintWithMultipleMatchPatternsFails) {
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
    ASSERT_EQ(swHinted.getValue()->getConfig().getIssuer(), kIssuer2);
}

TEST(IDPManager, refreshIDPKeys) {
    IDPConfiguration idpConfig;
    idpConfig.setIssuer(kIssuer1);
    idpConfig.setMatchPattern("@mongodb.com$"_sd);

    // Set the JWKSetFetcherFactory to fail initially, which should result in no keys loaded to the
    // IdentityProvider.
    auto uniqueFetcherFactory = std::make_unique<MockJWKSFetcherFactory>();
    auto* fetcherFactory = uniqueFetcherFactory.get();
    fetcherFactory->setShouldFail(true);
    IDPManager idpManager(std::move(uniqueFetcherFactory));
    idpManager.updateConfigurations(nullptr, {std::move(idpConfig)});

    // Assert that the IdentityProvider initially has no keys due to the failed fetch.
    BSONObjBuilder initialKeySetBob;
    idpManager.getIDP(kIssuer1).getValue()->serializeJWKSet(&initialKeySetBob);
    auto initialKeySet = initialKeySetBob.obj();

    ASSERT_BSONOBJ_EQ(initialKeySet, BSON("keys" << BSONArray()));

    // Now, allow the fetcher to start succeeding. The successful refresh should result in the keys
    // being properly loaded into the IdentityProvider's JWKManager.
    fetcherFactory->setShouldFail(false);
    ASSERT_OK(idpManager.getIDP(kIssuer1).getValue()->refreshKeys(
        *fetcherFactory, IdentityProvider::RefreshOption::kNow));

    BSONObjBuilder successfulRefreshKeySetBob;
    idpManager.getIDP(kIssuer1).getValue()->serializeJWKSet(&successfulRefreshKeySetBob);
    auto successfulRefreshKeySet = successfulRefreshKeySetBob.obj();
    auto testJWKSet = fetcherFactory->getTestJWKSet();

    ASSERT_BSONOBJ_EQ(successfulRefreshKeySet, testJWKSet);

    // Simulate a failed refresh. The keys should remain unchanged.
    fetcherFactory->setShouldFail(true);
    ASSERT_NOT_OK(idpManager.getIDP(kIssuer1).getValue()->refreshKeys(
        *fetcherFactory, IdentityProvider::RefreshOption::kNow));

    BSONObjBuilder failedRefreshKeySetBob;
    idpManager.getIDP(kIssuer1).getValue()->serializeJWKSet(&failedRefreshKeySetBob);
    auto failedRefreshKeySet = failedRefreshKeySetBob.obj();

    ASSERT_BSONOBJ_EQ(failedRefreshKeySet, testJWKSet);
}

BSONObj makeHumanIssuerBSON(StringData issuer, bool matcher) {
    BSONObjBuilder builder;
    builder.append("issuer", issuer);
    builder.append("audience", "mongo");
    if (matcher) {
        builder.append("matchPattern", ".*@mongodb.com");
    }
    builder.append("authNamePrefix", "prefix");
    builder.append("authorizationClaim", "groups");
    builder.append("clientId", "foo");
    builder.append("supportsHumanFlows", true);
    return builder.obj();
}

BSONObj makeMachineIssuerBSON(StringData issuer) {
    BSONObjBuilder builder;
    builder.append("issuer", issuer);
    builder.append("audience", "mongo");
    builder.append("authNamePrefix", "prefix");
    builder.append("authorizationClaim", "groups");
    builder.append("supportsHumanFlows", false);
    return builder.obj();
}

TEST(IDPManager, oneHumanIdPWithoutMatchers) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration = BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, twoHumanIdPsWithoutMatchersFail) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false) << makeHumanIssuerBSON(kIssuer2, false));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, twoHumanIdPsWithOneMatcherFails) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true) << makeHumanIssuerBSON(kIssuer2, false));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, oneMachineIdP) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration = BSON_ARRAY(makeMachineIssuerBSON(kIssuer1));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, twoMachineIdPs) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeMachineIssuerBSON(kIssuer1) << makeMachineIssuerBSON(kIssuer2));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, oneHumanOneMachineIdPsWithoutMatchers) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false) << makeMachineIssuerBSON(kIssuer2));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, twoHumanOneMachineIdPsWithoutMatchersFail) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, false)
                   << makeHumanIssuerBSON(kIssuer2, false) << makeMachineIssuerBSON(kIssuer3));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, twoHumanOneMachineIdPsWithOneMatchersFail) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true)
                   << makeHumanIssuerBSON(kIssuer2, false) << makeMachineIssuerBSON(kIssuer3));

    ASSERT_THROWS_WHAT(
        IDPManager::parseConfigFromBSONObj(configuration), DBException, "Required matchValue");
}

TEST(IDPManager, twoHumanOneMachineIdPsWithMatchers) {
    RAIIServerParameterControllerForTest featureFlagController(
        "featureFlagOIDCInternalAuthorization", true);

    auto configuration =
        BSON_ARRAY(makeHumanIssuerBSON(kIssuer1, true)
                   << makeHumanIssuerBSON(kIssuer2, true) << makeMachineIssuerBSON(kIssuer3));

    std::vector<IDPConfiguration> object = IDPManager::parseConfigFromBSONObj(configuration);
}

TEST(IDPManager, matchersMustBeFirst) {
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
