/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/auth/sasl_mechanism_policies.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"

namespace mongo {

class SaslAWSServerMechanism final : public MakeServerMechanism<AWSIAMPolicy> {
public:
    explicit SaslAWSServerMechanism(std::string authenticationDatabase)
        : MakeServerMechanism<AWSIAMPolicy>(std::move(authenticationDatabase)) {}

    ~SaslAWSServerMechanism() final = default;

    void appendExtraInfo(BSONObjBuilder* bob) const override;

    StatusWith<std::tuple<bool, std::string>> stepImpl(OperationContext* opCtx,
                                                       StringData inputData) override;

    boost::optional<unsigned int> currentStep() const override {
        return _step;
    }

    boost::optional<unsigned int> totalSteps() const override {
        return _maxStep;
    }

private:
    /**
     * Provide salt and nonce to client
     **/
    StatusWith<std::tuple<bool, std::string>> _firstStep(OperationContext* opCtx, StringData input);

    /**
     * Verify credentials
     **/
    StatusWith<std::tuple<bool, std::string>> _secondStep(OperationContext* opCtx,
                                                          StringData input);

private:
    const unsigned int _maxStep = 2;

    unsigned int _step{0};

    // Server generated nonce
    std::vector<char> _serverNonce;

    // Client provided Channel Binding Flag
    char _cbFlag{0};

    std::string _awsId;
    std::string _awsFullArn;
};

class AWSServerFactory : public MakeServerFactory<SaslAWSServerMechanism> {
public:
    using MakeServerFactory<SaslAWSServerMechanism>::MakeServerFactory;
    static constexpr bool isInternal = false;
    bool canMakeMechanismForUser(const User* user) const final {
        auto credentials = user->getCredentials();
        return credentials.isExternal;
    }
};

}  // namespace mongo
