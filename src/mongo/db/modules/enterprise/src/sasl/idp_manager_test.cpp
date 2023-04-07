/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/unittest/unittest.h"

#include "mongo/crypto/jwks_fetcher_mock.h"

#include "sasl/idp_manager.h"

namespace mongo::auth {
namespace {

constexpr auto kIssuer1 = "https://test.kernel.mongodb.com/IDPManager1"_sd;
constexpr auto kIssuer2 = "https://test.kernel.mongodb.com/IDPManager2"_sd;

class MockJWKSFetcherFactory : public JWKSFetcherFactory {
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
        return std::make_unique<crypto::MockJWKSFetcher>(getTestJWKSet());
    }
};

TEST(IDPManager, singleIDP) {
    IDPConfiguration idpc;
    idpc.setIssuer(kIssuer1);

    IDPManager idpm(std::make_unique<MockJWKSFetcherFactory>());
    ASSERT_OK(idpm.updateConfigurations(nullptr, {std::move(idpc)}));

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
    ASSERT_OK(idpm.updateConfigurations(nullptr, {std::move(issuer1), std::move(issuer2)}));

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

}  // namespace
}  // namespace mongo::auth
