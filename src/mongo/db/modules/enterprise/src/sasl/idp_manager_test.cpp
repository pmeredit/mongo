/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/unittest/unittest.h"

#include "sasl/idp_manager.h"

namespace mongo::auth {
namespace {

constexpr auto kJWKURL = "https://mongodbcorp.okta.com/oauth2/ausfgfhg2j9rtr0nT297/v1/keys"_sd;
constexpr auto kIssuer1 = "https://test.kernel.mongodb.com/IDPManager1"_sd;
constexpr auto kIssuer2 = "https://test.kernel.mongodb.com/IDPManager2"_sd;

TEST(IDPManager, singleIDP) {
    IDPConfiguration idpc;
    idpc.setJWKSUri(kJWKURL);
    idpc.setIssuer(kIssuer1);

    IDPManager idpm;
    ASSERT_OK(idpm.updateConfigurations(nullptr, {std::move(idpc)}));

    // Get Issuer by name.
    ASSERT_OK(idpm.getIDP(kIssuer1));
    ASSERT_NOT_OK(idpm.getIDP(kIssuer2));

    // Get one and only configured issuer.
    ASSERT_OK(idpm.selectIDP(boost::none));
}

TEST(IDPManager, multipleIDPs) {
    IDPConfiguration issuer1;
    issuer1.setJWKSUri(kJWKURL);
    issuer1.setIssuer(kIssuer1);
    issuer1.setMatchPattern("@mongodb.com$"_sd);

    IDPConfiguration issuer2;
    issuer2.setJWKSUri(kJWKURL);
    issuer2.setIssuer(kIssuer2);
    issuer2.setMatchPattern("@10gen.com$"_sd);

    IDPManager idpm;
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
