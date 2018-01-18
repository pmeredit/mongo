/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include "mongo/db/storage/data_protector.h"

#include <memory>

#include "mongo/base/status.h"

#include "symmetric_crypto.h"
#include "symmetric_key.h"

namespace mongo {

const uint8_t DATA_PROTECTOR_VERSION_0 = 0;

class EncryptedDataProtector : public DataProtector {
public:
    EncryptedDataProtector(const SymmetricKey* key, crypto::aesMode mode);
    Status protect(const std::uint8_t* in,
                   std::size_t inLen,
                   std::uint8_t* out,
                   std::size_t outLen,
                   std::size_t* bytesWritten) override;
    Status finalize(std::uint8_t* out, std::size_t outLen, std::size_t* bytesWritten) override;
    std::size_t getNumberOfBytesReservedForTag() const override;
    Status finalizeTag(std::uint8_t* out, std::size_t outLen, std::size_t* bytesWritten) override;

private:
    const SymmetricKey* _key;
    const crypto::aesMode _mode;
    std::unique_ptr<crypto::SymmetricEncryptor> _encryptor;
};

}  // namespace mongo
