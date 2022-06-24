/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <string>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"

namespace mongo {
constexpr auto kOIDCMechanismName = "MONGODB-OIDC"_sd;

struct OIDCPolicy {
    static constexpr StringData getName() {
        return kOIDCMechanismName;
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

    StatusWith<StepTuple> stepImpl(OperationContext*, StringData inputData);

private:
    StepTuple _step1(BSONObj payload);
    StepTuple _step2(BSONObj payload);

    int _step{0};
};

class OIDCServerFactory : public MakeServerFactory<SaslOIDCServerMechanism> {
public:
    using MakeServerFactory<SaslOIDCServerMechanism>::MakeServerFactory;
    static constexpr bool isInternal = policy_type::isInternalAuthMech();
    bool canMakeMechanismForUser(const User* user) const final;
};

}  // namespace mongo
