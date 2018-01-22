/**
 *    Copyright (C) 2018 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <CommonCrypto/CommonCryptor.h>
#include <Security/Security.h>
#include <set>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

#include "symmetric_crypto.h"
#include "symmetric_key.h"

namespace mongo {
namespace crypto {

namespace {

template <typename Parent>
class SymmetricImplApple : public Parent {
public:
    SymmetricImplApple(const SymmetricKey& key, aesMode mode, const uint8_t* iv, size_t ivLen)
        : _ctx(nullptr, CCCryptorRelease) {
        static_assert(
            std::is_same<Parent, SymmetricEncryptor>::value ||
                std::is_same<Parent, SymmetricDecryptor>::value,
            "SymmetricImplApple must inherit from SymmetricEncryptor or SymmetricDecryptor");

        uassert(ErrorCodes::UnsupportedFormat,
                "Native crypto on this platform only supports AES256-CBC",
                mode == aesMode::cbc);

        // Note: AES256 uses a 256byte keysize,
        // but is still functionally a 128bit block algorithm.
        // Therefore we expect a 128 bit block length.
        uassert(ErrorCodes::BadValue,
                str::stream() << "Invalid ivlen for selected algorithm, expected "
                              << kCCBlockSizeAES128
                              << ", got "
                              << ivLen,
                ivLen == kCCBlockSizeAES128);

        CCCryptorRef context = nullptr;
        constexpr auto op =
            std::is_same<Parent, SymmetricEncryptor>::value ? kCCEncrypt : kCCDecrypt;
        const auto status = CCCryptorCreate(op,
                                            kCCAlgorithmAES,
                                            kCCOptionPKCS7Padding,
                                            key.getKey(),
                                            key.getKeySize(),
                                            iv,
                                            &context);
        uassert(ErrorCodes::UnknownError,
                str::stream() << "CCCryptorCreate failure: " << status,
                status == kCCSuccess);

        _ctx.reset(context);
    }

    StatusWith<size_t> update(const uint8_t* in, size_t inLen, uint8_t* out, size_t outLen) final {
        size_t outUsed = 0;
        const auto status = CCCryptorUpdate(_ctx.get(), in, inLen, out, outLen, &outUsed);
        if (status != kCCSuccess) {
            return Status(ErrorCodes::UnknownError,
                          str::stream() << "Unable to perform CCCryptorUpdate: " << status);
        }
        return outUsed;
    }

    StatusWith<size_t> finalize(uint8_t* out, size_t outLen) final {
        size_t outUsed = 0;
        const auto status = CCCryptorFinal(_ctx.get(), out, outLen, &outUsed);
        if (status != kCCSuccess) {
            return Status(ErrorCodes::UnknownError,
                          str::stream() << "Unable to perform CCCryptorFinal: " << status);
        }
        return outUsed;
    }

private:
    std::unique_ptr<_CCCryptor, decltype(&CCCryptorRelease)> _ctx;
};

class SymmetricEncryptorApple : public SymmetricImplApple<SymmetricEncryptor> {
public:
    using SymmetricImplApple::SymmetricImplApple;

    StatusWith<size_t> finalizeTag(uint8_t* out, size_t outLen) final {
        // CBC only, no tag to create.
        return 0;
    }
};


class SymmetricDecryptorApple : public SymmetricImplApple<SymmetricDecryptor> {
public:
    using SymmetricImplApple::SymmetricImplApple;

    Status updateTag(const uint8_t* tag, size_t tagLen) final {
        // CBC only, no tag to verify.
        if (tagLen > 0) {
            return {ErrorCodes::BadValue, "Unexpected tag for non-gcm cipher"};
        }
        return Status::OK();
    }
};

}  // namespace

std::set<std::string> getSupportedSymmetricAlgorithms() {
    return {aes256CBCName};
}

Status engineRandBytes(uint8_t* buffer, size_t len) {
    auto result = SecRandomCopyBytes(kSecRandomDefault, len, buffer);
    if (result != errSecSuccess) {
        return {ErrorCodes::UnknownError,
                str::stream() << "Failed generating random bytes: " << result};
    } else {
        return Status::OK();
    }
}

StatusWith<std::unique_ptr<SymmetricEncryptor>> SymmetricEncryptor::create(const SymmetricKey& key,
                                                                           aesMode mode,
                                                                           const uint8_t* iv,
                                                                           size_t ivLen) try {
    std::unique_ptr<SymmetricEncryptor> encryptor =
        stdx::make_unique<SymmetricEncryptorApple>(key, mode, iv, ivLen);
    return std::move(encryptor);
} catch (const DBException& e) {
    return e.toStatus();
}

StatusWith<std::unique_ptr<SymmetricDecryptor>> SymmetricDecryptor::create(const SymmetricKey& key,
                                                                           aesMode mode,
                                                                           const uint8_t* iv,
                                                                           size_t ivLen) try {
    std::unique_ptr<SymmetricDecryptor> decryptor =
        std::make_unique<SymmetricDecryptorApple>(key, mode, iv, ivLen);
    return std::move(decryptor);
} catch (const DBException& e) {
    return e.toStatus();
}

}  // namespace crypto
}  // namespace mongo
