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

    StatusWith<std::tuple<bool, std::string>> stepImpl(OperationContext* opCtx,
                                                       StringData inputData);

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
    int _step{0};

    // Server generated nonce
    std::vector<char> _serverNonce;

    // Client provided Channel Binding Flag
    char _cbFlag{0};
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
