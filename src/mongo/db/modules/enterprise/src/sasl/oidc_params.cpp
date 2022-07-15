/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "oidc_params.h"

#include <fstream>

#include "mongo/base/init.h"
#include "mongo/bson/json.h"
#include "mongo/crypto/jwk_manager.h"

namespace mongo {
namespace {
crypto::JWKManager oidcKeyManager;

MONGO_INITIALIZER(OIDCKeyManager)(InitializerContext*) {
    if (oidcGlobalParams.keySetFile.empty()) {
        return;
    }

    std::ifstream keyFile(oidcGlobalParams.keySetFile, std::ios_base::in);
    uassert(ErrorCodes::OperationFailed,
            str::stream() << "Failed opening OID key file: " << oidcGlobalParams.keySetFile,
            keyFile.is_open());

    BSONObj keyData;
    try {
        std::string data((std::istreambuf_iterator<char>(keyFile)),
                         std::istreambuf_iterator<char>());
        keyData = fromjson(data);
    } catch (const DBException& ex) {
        uassertStatusOK(ex.toStatus().withContext(
            str::stream() << "Invalid JSON in '" << oidcGlobalParams.keySetFile << "'"));
    }

    oidcKeyManager = crypto::JWKManager(keyData);
}
}  // namespace

OIDCGlobalParams oidcGlobalParams;

crypto::JWKManager* getOIDCKeyManager() {
    return &oidcKeyManager;
}

}  // namespace mongo
