/**
 * Copyright (c) 2019 MongoDB, Inc.
 */

#include "mongo/platform/basic.h"

#include "boost/filesystem.hpp"

#include "encryption_options.h"
#include "keystore_metadata.h"
#include "symmetric_crypto.h"
#include "symmetric_key.h"

#include "mongo/unittest/unittest.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
namespace crypto {

class UniquePath {
public:
    ~UniquePath() {
        if (boost::filesystem::exists(_path)) {
            invariant(boost::filesystem::remove(_path));
        }
    }

    operator const boost::filesystem::path&() const {
        return _path;
    }

private:
    boost::filesystem::path _path = boost::filesystem::unique_path();
};

TEST(KeystoreMetadataFile, DefaultParams) {
    UniqueSymmetricKey key = std::make_unique<SymmetricKey>(aesGenerate(sym256KeySize, "testID"));
    EncryptionGlobalParams params;
    UniquePath keystorePath;

    auto swKeystore = KeystoreMetadataFile::load(keystorePath, key, params);

    ASSERT_EQ(swKeystore.getStatus(), ErrorCodes::NonExistentPath);

    KeystoreMetadataFile file;
    file.setVersion(500);
    file.setDirty(true);

    ASSERT_OK(file.store(keystorePath, key, params));

    auto loadedKeystore =
        unittest::assertGet(KeystoreMetadataFile::load(keystorePath, key, params));
    ASSERT_EQ(loadedKeystore.getVersion(), file.getVersion());
    ASSERT_EQ(loadedKeystore.getDirty(), file.getDirty());
}

TEST(KeystoreMetadataFile, GCM) {
    auto supportedAlgos = getSupportedSymmetricAlgorithms();
    if (supportedAlgos.find("AES256-GCM") == supportedAlgos.end()) {
        return;
    }

    UniqueSymmetricKey key = std::make_unique<SymmetricKey>(aesGenerate(sym256KeySize, "testID"));
    EncryptionGlobalParams params;
    params.encryptionCipherMode = "AES256-GCM";
    UniquePath keystorePath;

    auto swKeystore = KeystoreMetadataFile::load(keystorePath, key, params);

    ASSERT_EQ(swKeystore.getStatus(), ErrorCodes::NonExistentPath);

    KeystoreMetadataFile file;
    file.setVersion(500);
    file.setDirty(true);

    ASSERT_OK(file.store(keystorePath, key, params));

    auto loadedKeystore =
        unittest::assertGet(KeystoreMetadataFile::load(keystorePath, key, params));
    ASSERT_EQ(loadedKeystore.getVersion(), file.getVersion());
    ASSERT_EQ(loadedKeystore.getDirty(), file.getDirty());
}

TEST(KeystoreMetadataFile, DefaultValues) {
    UniqueSymmetricKey key = std::make_unique<SymmetricKey>(aesGenerate(sym256KeySize, "testID"));
    EncryptionGlobalParams params;
    UniquePath keystorePath;

    KeystoreMetadataFile file;
    ASSERT_OK(file.store(keystorePath, key, params));

    auto loadedKeystore =
        unittest::assertGet(KeystoreMetadataFile::load(keystorePath, key, params));
    ASSERT_EQ(loadedKeystore.getVersion(), 0);
    ASSERT_EQ(loadedKeystore.getDirty(), false);
}

}  // namespace crypto
}  // namespace mongo
