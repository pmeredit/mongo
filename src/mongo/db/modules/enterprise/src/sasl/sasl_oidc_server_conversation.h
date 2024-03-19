/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/client/authenticate.h"
#include "mongo/crypto/jws_validated_token.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"
#include "sasl/idp_manager.h"

namespace mongo {
class Client;

namespace auth {
struct OIDCPolicy {
    static constexpr StringData getName() {
        return kMechanismMongoOIDC;
    }
    static SecurityPropertySet getProperties() {
        return SecurityPropertySet{SecurityProperty::kNoPlainText};
    }
    static int securityLevel() {
        return 1;
    }
    static constexpr bool isInternalAuthMech() {
        return false;
    }
};

/**
 *  Server side authentication session for SASL MONGODB-OIDC
 */
class SaslOIDCServerMechanism final : public MakeServerMechanism<OIDCPolicy> {
private:
    using StepTuple = std::tuple<bool, std::string>;

public:
    using MakeServerMechanism<OIDCPolicy>::MakeServerMechanism;

    StatusWith<StepTuple> stepImpl(OperationContext*, StringData inputData) override;

    boost::optional<unsigned int> currentStep() const override {
        return _step;
    }

    boost::optional<unsigned int> totalSteps() const override {
        return 2;
    }

    UserRequest getUserRequest() const override;
    boost::optional<Date_t> getExpirationTime() const override {
        return _expirationTime;
    }

    void appendExtraInfo(BSONObjBuilder* builder) const override {
        builder->appendElements(_extraInfo);
    }

private:
    StepTuple _step1(OperationContext*, BSONObj payload);
    StepTuple _step2(OperationContext*, BSONObj payload);

    unsigned int _step{0};
    boost::optional<std::string> _principalNameHint;
    IDPManager::SharedIdentityProvider _idp;
    std::string _mechanismData;
    Date_t _expirationTime;
    BSONObj _extraInfo;
};

class OIDCServerFactory : public MakeServerFactory<SaslOIDCServerMechanism> {
public:
    using MakeServerFactory<SaslOIDCServerMechanism>::MakeServerFactory;
    static constexpr bool isInternal = policy_type::isInternalAuthMech();
    bool canMakeMechanismForUser(const User* user) const final;
};

}  // namespace auth
}  // namespace mongo
