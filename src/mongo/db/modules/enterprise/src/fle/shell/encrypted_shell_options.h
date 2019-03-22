/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */
#pragma once
#include <string>

namespace mongo {

struct EncryptedShellGlobalParams {
    std::string awsAccessKeyId;
    std::string awsSecretAccessKey;
    std::string awsSessionToken;
};

extern EncryptedShellGlobalParams encryptedShellGlobalParams;
}
