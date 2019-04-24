/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */


#include <kms_message/kms_message.h>

#include <stdlib.h>

#include "mongo/base/init.h"
#include "mongo/base/secure_allocator.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/json.h"
#include "mongo/util/base64.h"

#include "encryptdb/symmetric_crypto.h"
#include "encryptdb/symmetric_key.h"
#include "fle/encryption/aead_encryption.h"
#include "fle/shell/kms_gen.h"
#include "kms.h"

namespace mongo {
namespace {

/**
 * Manages Local KMS Information
 */
class LocalKMSService : public KMSService {
public:
    LocalKMSService(SymmetricKey key) : _key(std::move(key)) {}
    ~LocalKMSService() final = default;

    static std::unique_ptr<KMSService> create(const LocalKMS& config);

    std::vector<uint8_t> encrypt(ConstDataRange cdr, StringData kmsKeyId) final;

    SecureVector<uint8_t> decrypt(ConstDataRange cdr, BSONObj masterKey) final;

    BSONObj encryptDataKey(ConstDataRange cdr, StringData keyId) final;

private:
    // Key that wraps all KMS encrypted data
    SymmetricKey _key;
};

std::vector<uint8_t> LocalKMSService::encrypt(ConstDataRange cdr, StringData kmsKeyId) {
    std::vector<std::uint8_t> ciphertext(crypto::aeadCipherOutputLength(cdr.length()));

    uassertStatusOK(crypto::aeadEncrypt(_key,
                                        reinterpret_cast<const uint8_t*>(cdr.data()),
                                        cdr.length(),
                                        nullptr,
                                        0,
                                        ciphertext.data(),
                                        ciphertext.size()));

    return ciphertext;
}

BSONObj LocalKMSService::encryptDataKey(ConstDataRange cdr, StringData keyId) {
    auto dataKey = encrypt(cdr, keyId);

    LocalMasterKey masterKey;

    LocalMasterKeyAndMaterial keyAndMaterial;
    keyAndMaterial.setKeyMaterial(dataKey);
    keyAndMaterial.setMasterKey(masterKey);

    return keyAndMaterial.toBSON();
}

SecureVector<uint8_t> LocalKMSService::decrypt(ConstDataRange cdr, BSONObj masterKey) {
    SecureVector<uint8_t> plaintext(cdr.length());

    size_t outLen = plaintext->size();
    uassertStatusOK(crypto::aeadDecrypt(_key,
                                        reinterpret_cast<const uint8_t*>(cdr.data()),
                                        cdr.length(),
                                        nullptr,
                                        0,
                                        plaintext->data(),
                                        &outLen));
    plaintext->resize(outLen);

    return plaintext;
}

std::unique_ptr<KMSService> LocalKMSService::create(const LocalKMS& config) {
    uassert(1,
            str::stream() << "Local KMS key must be 64 bytes, found " << config.getKey().length()
                          << " bytes instead",
            config.getKey().length() == crypto::kAeadAesHmacKeySize);

    SecureVector<uint8_t> aesVector = SecureVector<uint8_t>(
        config.getKey().data(), config.getKey().data() + config.getKey().length());
    SymmetricKey key = SymmetricKey(aesVector, crypto::aesAlgorithm, "local");

    auto localKMS = std::make_unique<LocalKMSService>(std::move(key));

    return localKMS;
}

/**
 * Factory for LocalKMSService if user specifies local config to mongo() JS constructor.
 */
class LocalKMSServiceFactory final : public KMSServiceFactory {
public:
    LocalKMSServiceFactory() = default;
    ~LocalKMSServiceFactory() = default;

    std::unique_ptr<KMSService> create(const BSONObj& config) final {
        auto field = config[KmsProviders::kLocalFieldName];
        if (field.eoo()) {
            return nullptr;
        }

        auto obj = field.Obj();
        return LocalKMSService::create(LocalKMS::parse(IDLParserErrorContext("root"), obj));
    }
};

}  // namspace

MONGO_INITIALIZER(LocalKMSRegister)(::mongo::InitializerContext* context) {
    KMSServiceController::registerFactory(KMSProviderEnum::local,
                                          std::make_unique<LocalKMSServiceFactory>());
    return Status::OK();
}

}  // namespace mongo
