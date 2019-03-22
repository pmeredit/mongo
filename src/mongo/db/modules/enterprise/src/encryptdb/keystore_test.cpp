/**
 * Copyright (c) 2019 MongoDB, Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include <boost/optional/optional_io.hpp>

#include "encryption_key_manager_noop.h"
#include "encryption_options.h"
#include "keystore.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_extensions.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"


namespace mongo {
namespace {
ServiceContext::ConstructorActionRegisterer registerEncryptionWiredTigerCustomizationHooks{
    "CreateEncryptionWiredTigerCustomizationHooks",
    {"SetWiredTigerCustomizationHooks", "SecureAllocator", "CreateKeyEntropySource"},
    [](ServiceContext* service) {
        auto configHooks =
            stdx::make_unique<EncryptionWiredTigerCustomizationHooks>(&encryptionGlobalParams);
        WiredTigerCustomizationHooks::set(service, std::move(configHooks));
        auto keyManager = stdx::make_unique<EncryptionKeyManagerNoop>();
        EncryptionHooks::set(service, std::move(keyManager));
        WiredTigerExtensions::get(service)->addExtension(mongo::kEncryptionEntrypointConfig);
    }};


class KeystoreFixture : public ServiceContextTest {
public:
    KeystoreFixture() : _keystorePath("keystore") {}

    void setUp() {
        encryptionGlobalParams.encryptionCipherMode = "AES256-GCM";
        encryptionGlobalParams.enableEncryption = true;
    }

protected:
    boost::filesystem::path keystorePath() {
        return boost::filesystem::path(_keystorePath.path());
    }

    static bool gcmSupported() {
        static const auto isGcmSupported = [] {
            const auto supportedList = crypto::getSupportedSymmetricAlgorithms();
            return supportedList.find(crypto::aes256GCMName) != supportedList.end();
        }();
        return isGcmSupported;
    }

    struct KeystoreAndSession {
        KeystoreAndSession(std::unique_ptr<Keystore> keystore,
                           std::unique_ptr<Keystore::Session> session)
            : keystore(std::move(keystore)), session(std::move(session)) {}

        std::unique_ptr<Keystore> keystore;
        std::unique_ptr<Keystore::Session> session;

        void reset() {
            session.reset();
            keystore.reset();
        }
    };

    KeystoreAndSession makeKeystoreAndSession(Keystore::Version version) {
        auto keystore = Keystore::makeKeystore(
            boost::filesystem::path(keystorePath()), version, &encryptionGlobalParams);
        auto session = keystore->makeSession();

        return KeystoreAndSession(std::move(keystore), std::move(session));
    }

    std::unique_ptr<SymmetricKey> makeKey(StringData name) {
        return stdx::make_unique<SymmetricKey>(crypto::aesGenerate(crypto::sym256KeySize, name));
    }

private:
    unittest::TempDir _keystorePath;
};

// Check the basic create/read/update functionality of the v0 keystore.
TEST_F(KeystoreFixture, V0KeystoreCRUTest) {
    encryptionGlobalParams.encryptionCipherMode = "AES256-CBC";
    auto version = Keystore::Version::k0;
    auto ks = makeKeystoreAndSession(version);
    auto newKey = makeKey("a");

    ks.session->insert(newKey);
    ASSERT_EQ(newKey->getKeyId().id(), boost::none);
    ks.reset();

    ks = makeKeystoreAndSession(version);
    auto it = ks.session->begin();
    ASSERT_EQ(it->getKeyId().name(), "a");

    auto key = std::move(*it);
    auto oldCount = key->getInitializationCount();
    ASSERT_EQ(oldCount, static_cast<uint32_t>(1));
    key->incrementAndGetInitializationCount();
    ks.session->update(std::move(it), key);

    it = ks.session->begin();
    ASSERT_EQ(it->getInitializationCount(), static_cast<uint32_t>(2));
}

// Check the basic create/read/update functionality of the v1 keystore.
TEST_F(KeystoreFixture, V1KeystoreCRUTest) {
    if (!gcmSupported()) {
        log() << "Test requires GCM";
        return;
    }

    auto version = Keystore::Version::k1;
    auto ks = makeKeystoreAndSession(version);
    auto newKey = makeKey("a");

    ks.session->insert(newKey);
    ASSERT_EQ(newKey->getKeyId().id(), static_cast<uint64_t>(1));
    ks.reset();

    ks = makeKeystoreAndSession(version);
    auto it = ks.session->begin();
    ASSERT_EQ(it->getKeyId().name(), "a");

    auto key = std::move(*it);
    auto oldCount = key->getInitializationCount();
    ASSERT_EQ(oldCount, static_cast<uint32_t>(1));
    key->incrementAndGetInitializationCount();
    ks.session->update(std::move(it), key);

    it = ks.session->begin();
    ASSERT_EQ(it->getInitializationCount(), static_cast<uint32_t>(2));
}

TEST_F(KeystoreFixture, V1RolloverTest) {
    if (!gcmSupported()) {
        log() << "Test requires GCM";
        return;
    }

    auto ks = makeKeystoreAndSession(Keystore::Version::k1);

    auto aKey = makeKey("a");
    ks.session->insert(aKey);
    auto bKey = makeKey("b");
    ks.session->insert(bKey);

    // Check that we can find the "a" key by name
    auto it = ks.session->find(SymmetricKeyId("a"));
    ASSERT_FALSE(it == ks.session->end());
    aKey->incrementAndGetInitializationCount();
    ks.session->update(std::move(it), aKey);

    // Check that we can find the "b" key by numeric ID
    it = ks.session->find(bKey->getKeyId());
    ASSERT_FALSE(it == ks.session->end());

    // roll over the keys (really just flush the name lookup cache)
    ks.keystore->rollOverKeys();

    // We should no longer be able to lookup the key by name only
    it = ks.session->find(SymmetricKeyId("a"));
    ASSERT_TRUE(it == ks.session->end());

    // but we should still be able to lookup the key by numeric ID
    it = ks.session->find(bKey->getKeyId());
    ASSERT_FALSE(it == ks.session->end());
    ASSERT_EQ(it->getKeyId().id(), static_cast<uint64_t>(2));

    // Add a new "a" key that will have its own numeric ID
    auto newAKey = makeKey("a");
    ks.session->insert(newAKey);

    // We should be able to do name-based lookups for "a" now.
    it = ks.session->find(SymmetricKeyId("a"));
    ASSERT_FALSE(it == ks.session->end());
    ASSERT_EQ(it->getInitializationCount(), static_cast<uint32_t>(1));

    // Check that this is the new "a" key and not the old one
    ASSERT_FALSE(aKey->getKeyId() == it->getKeyId());
    ASSERT_GT(it->getKeyId().id(), aKey->getKeyId().id());
    auto secondIt = ks.session->find(aKey->getKeyId());
    ASSERT_FALSE(it == secondIt);
    ASSERT_FALSE(secondIt == ks.session->end());
}

// Rolling over a V0 keystore should be fatal
DEATH_TEST_F(KeystoreFixture, V0RolloverTest, "Fatal Assertion 51160") {
    encryptionGlobalParams.encryptionCipherMode = "AES256-CBC";
    auto ks = makeKeystoreAndSession(Keystore::Version::k0);
    ks.keystore->rollOverKeys();
}

DEATH_TEST_F(KeystoreFixture, V1CBCTest, "Fatal Assertion 51163") {
    encryptionGlobalParams.encryptionCipherMode = "AES256-CBC";
    auto ks = makeKeystoreAndSession(Keystore::Version::k1);
}

}  // namespace
}  // namespace mongo
