/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include "encryption_key_manager_noop.h"
#include "encryption_options.h"
#include "keystore.h"
#include "mongo/config.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_extensions.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault


namespace mongo {
namespace {

template <typename ManagerImpl, typename... Args>
void setupEncryption(ServiceContext* service, Args... args) {
    auto configHooks =
        std::make_unique<EncryptionWiredTigerCustomizationHooks>(&encryptionGlobalParams);
    WiredTigerCustomizationHooks::set(service, std::move(configHooks));
    auto keyManager = std::make_unique<ManagerImpl>(args...);
    EncryptionHooks::set(service, std::move(keyManager));
    WiredTigerExtensions::get(service)->addExtension(mongo::kEncryptionEntrypointConfig);
}

ServiceContext::ConstructorActionRegisterer registerEncryptionWiredTigerCustomizationHooks{
    "CreateEncryptionWiredTigerCustomizationHooks",
    {"SetWiredTigerCustomizationHooks", "SecureAllocator"},
    [](ServiceContext* service) { setupEncryption<EncryptionKeyManagerNoop>(service); }};


class KeystoreFixture : public ServiceContextTest {
public:
    KeystoreFixture() : _keystorePath("keystore") {}

    void setUp() override {
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
        return std::make_unique<SymmetricKey>(crypto::aesGenerate(crypto::sym256KeySize, name));
    }

    void testSalvageFixKeystore(Keystore::Version version, std::string encryptionCipherMode);

private:
    unittest::TempDir _keystorePath;
};

void KeystoreFixture::testSalvageFixKeystore(Keystore::Version version,
                                             std::string encryptionCipherMode) {
    encryptionGlobalParams.encryptionCipherMode = encryptionCipherMode;
    encryptionGlobalParams.repair = true;

    auto ks = makeKeystoreAndSession(version);
    auto newKey = makeKey("a");

    ks.session->insert(newKey);
    ks.session->end();
    ks.reset();

    boost::filesystem::path keystoreTablePath = keystorePath() / "keystore.wt";

    ASSERT_TRUE(boost::filesystem::remove(keystoreTablePath));

    std::ofstream myfile;
    myfile.open(keystoreTablePath.string());
    myfile << "";
    myfile.close();

    // This is expected to fail as salvage() should fail to recover the corrupted keystore table
    auto ks1 = Keystore::makeKeystore(
        boost::filesystem::path(keystorePath()), version, &encryptionGlobalParams);
}

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
        LOGV2(24193, "Test requires GCM");
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
        LOGV2(24194, "Test requires GCM");
        return;
    }

    using FindMode = Keystore::Session::FindMode;

    auto ks = makeKeystoreAndSession(Keystore::Version::k1);

    auto aKey = makeKey("a");
    ks.session->insert(aKey);
    auto bKey = makeKey("b");
    ks.session->insert(bKey);

    // Check that we can find the "a" key by name
    auto it = ks.session->find(SymmetricKeyId("a"), FindMode::kCurrent);
    ASSERT_FALSE(it == ks.session->end());
    aKey->incrementAndGetInitializationCount();
    ks.session->update(std::move(it), aKey);

    // Check that we can find the "b" key by numeric ID
    it = ks.session->find(bKey->getKeyId(), FindMode::kIdOrCurrent);
    ASSERT_FALSE(it == ks.session->end());

    // roll over the keys (really just flush the name lookup cache)
    ks.keystore->rollOverKeys();

    // We should no longer be able to lookup the key by name only
    it = ks.session->find(SymmetricKeyId("a"), FindMode::kCurrent);
    ASSERT_TRUE(it == ks.session->end());

    // but we should still be able to lookup the key by numeric ID
    it = ks.session->find(bKey->getKeyId(), FindMode::kIdOrCurrent);
    ASSERT_FALSE(it == ks.session->end());
    ASSERT_EQ(it->getKeyId().id(), static_cast<uint64_t>(2));

    // Add a new "a" key that will have its own numeric ID
    auto newAKey = makeKey("a");
    ks.session->insert(newAKey);

    // We should be able to do name-based lookups for "a" now.
    it = ks.session->find(SymmetricKeyId("a"), FindMode::kCurrent);
    ASSERT_FALSE(it == ks.session->end());
    ASSERT_EQ(it->getInitializationCount(), static_cast<uint32_t>(1));

    // Check that this is the new "a" key and not the old one
    ASSERT_FALSE(aKey->getKeyId() == it->getKeyId());
    ASSERT_GT(it->getKeyId().id(), aKey->getKeyId().id());
    auto secondIt = ks.session->find(aKey->getKeyId(), FindMode::kIdOrCurrent);
    ASSERT_FALSE(it == secondIt);
    ASSERT_FALSE(secondIt == ks.session->end());
}

// Check that insertion with differing rolloverIds works as expected.
TEST_F(KeystoreFixture, V1InsertRolloverTest) {
    if (!gcmSupported()) {
        LOGV2(6307001, "Test requires GCM");
        return;
    }

    using FindMode = Keystore::Session::FindMode;

    auto ks = makeKeystoreAndSession(Keystore::Version::k1);
    uint32_t oldRolloverId = ks.keystore->getRolloverId();
    ks.keystore->rollOverKeys();
    uint32_t currentRolloverId = ks.keystore->getRolloverId();
    ASSERT_FALSE(oldRolloverId == currentRolloverId);

    auto aKey = makeKey("a");
    ks.session->insert(aKey);

    // Test that inserting a duplicate key with a different rolloverId is fine
    auto aKeyAgain = makeKey("a");
    ks.session->insert(aKeyAgain, oldRolloverId);

    // A name-based lookup should return the newer rollover ID entry.
    auto it = ks.session->find(aKey->getKeyId(), FindMode::kCurrent);
    ASSERT_FALSE(it == ks.session->end());
    ASSERT_EQ(it->getKeyId().id(), aKey->getKeyId().id());
    ASSERT_EQ(currentRolloverId, ks.session->getRolloverId(it.cursor()));

    // A name-based lookup when there's only an entry in the old rollover set should not find
    // anything.
    auto cKey = makeKey("c");
    ks.session->insert(cKey, oldRolloverId);
    it = ks.session->find(cKey->getKeyId(), FindMode::kCurrent);
    ASSERT_TRUE(it == ks.session->end());
}

// We require "AES256-GCM" to pass this test
#if MONGO_CONFIG_SSL_PROVIDER == MONGO_CONFIG_SSL_PROVIDER_OPENSSL
DEATH_TEST_REGEX_F(KeystoreFixture, V1SalvageFixKeystore2, "Fatal assertion.*51226") {
    KeystoreFixture::testSalvageFixKeystore(Keystore::Version::k1, "AES256-GCM");
}
#endif

DEATH_TEST_REGEX_F(KeystoreFixture, V0SalvageFixKeystore, "Fatal assertion.*51226") {
    KeystoreFixture::testSalvageFixKeystore(Keystore::Version::k0, "AES256-CBC");
}

// Rolling over a V0 keystore should be fatal
DEATH_TEST_REGEX_F(KeystoreFixture, V0RolloverTest, "Fatal assertion.*51168") {
    encryptionGlobalParams.encryptionCipherMode = "AES256-CBC";
    auto ks = makeKeystoreAndSession(Keystore::Version::k0);
    ks.keystore->rollOverKeys();
}

DEATH_TEST_REGEX_F(KeystoreFixture, V1CBCTest, "Fatal assertion.*51165") {
    encryptionGlobalParams.encryptionCipherMode = "AES256-CBC";
    auto ks = makeKeystoreAndSession(Keystore::Version::k1);
}

TEST(EncryptionKeyManager, HotBackupValidation) {
    // First set up the dbpath we're going to use.
    unittest::TempDir dbPathTempDir("keystoreHotBackups");
    boost::filesystem::path dbPath(dbPathTempDir.path());

    // Make sure it's absolute!
    if (!dbPath.is_absolute()) {
        dbPath = boost::filesystem::absolute(dbPath);
        ASSERT_TRUE(boost::filesystem::exists(dbPath));
    }

    const auto dbPathStr = dbPath.string();
    LOGV2(24195, "dbpath is {dbPathStr}", "dbPathStr"_attr = dbPathStr);

    // Create a key. This is the contents of jstests/encryptdb/libs/ekf
    boost::filesystem::path keyPath = dbPath / "keyFile";
    {
        std::ofstream keyPathOut(keyPath.c_str());
        keyPathOut << "YW1hbGlhaXNzb2Nvb2x5b2FtYWxpYWlzc29jb29seW8=";
    }

#ifndef _WIN32
    // Make sure the permissions are correct.
    boost::filesystem::permissions(keyPath, boost::filesystem::owner_read);
#endif

    // Setup the encryption params and create a ServiceContext to instantiate the storage
    // engine and everything else we need.
    encryptionGlobalParams.enableEncryption = true;
    encryptionGlobalParams.encryptionKeyFile = keyPath.string();

    setGlobalServiceContext(ServiceContext::make());
    const ScopeGuard serviceContextCleanup = [&] { setGlobalServiceContext({}); };

    auto const service = getGlobalServiceContext();
    // Override the Noop encryption manager with the real encryption manager.
    setupEncryption<EncryptionKeyManager>(service, dbPathStr, &encryptionGlobalParams);

    auto const encryptionManager = EncryptionKeyManager::get(service);

    // Make sure we can actually get some keys and that we've done some writes.
    auto systemKey = unittest::assertGet(encryptionManager->getKey(kSystemKeyId));
    auto tmpKey = unittest::assertGet(encryptionManager->getKey(kTmpDataKeyId));
    systemKey.reset();
    tmpKey.reset();

    // Get the list of files to backup.
    auto backupFiles = unittest::assertGet(encryptionManager->beginNonBlockingBackup());

    // Make sure the paths are absolute.
    stdx::unordered_set<std::string> expectedFiles;
    for (const auto& file : {"local/WiredTiger.backup",
                             "local/keystore.wt",
                             "local/WiredTiger",
                             "keystore.metadata"}) {
        auto filePath = dbPath / "key.store" / boost::filesystem::path(file).make_preferred();
        expectedFiles.insert(filePath.string());
    }

    for (const auto& file : backupFiles) {
        LOGV2(24196, "Hot backups would back up {file}", "file"_attr = file);
        ASSERT_TRUE(str::startsWith(file, dbPathStr));
        ASSERT_TRUE(boost::filesystem::exists(file));
        expectedFiles.erase(file);
    }

    for (const auto& missingFile : expectedFiles) {
        LOGV2_ERROR(24198, "Expected to find: {missingFile}", "missingFile"_attr = missingFile);
    }
    ASSERT_TRUE(expectedFiles.empty());

    ASSERT_OK(encryptionManager->endNonBlockingBackup());

    ASSERT_FALSE(boost::filesystem::exists(dbPath / "key.store/local/WiredTiger.backup"));
}

// This checks that opening a backup cursor when CBC mode is in use doesn't cause a
// rollover attempt on startup.
TEST(EncryptionKeyManager, DirtyCBCIsNoOp) {
    // First set up the dbpath we're going to use.
    unittest::TempDir dbPathTempDir("keystoreHotBackups");
    boost::filesystem::path dbPath(dbPathTempDir.path());

    // Make sure it's absolute!
    if (!dbPath.is_absolute()) {
        dbPath = boost::filesystem::absolute(dbPath);
        ASSERT_TRUE(boost::filesystem::exists(dbPath));
    }

    const auto dbPathStr = dbPath.string();
    LOGV2(24197, "dbpath is {dbPathStr}", "dbPathStr"_attr = dbPathStr);

    // Create a key. This is the contents of jstests/encryptdb/libs/ekf
    boost::filesystem::path keyPath = dbPath / "keyFile";
    {
        std::ofstream keyPathOut(keyPath.c_str());
        keyPathOut << "YW1hbGlhaXNzb2Nvb2x5b2FtYWxpYWlzc29jb29seW8=";
    }

#ifndef _WIN32
    // Make sure the permissions are correct.
    boost::filesystem::permissions(keyPath, boost::filesystem::owner_read);
#endif

    // Setup the encryption params and create a ServiceContext to instantiate the storage
    // engine and everything else we need.
    encryptionGlobalParams.enableEncryption = true;
    encryptionGlobalParams.encryptionKeyFile = keyPath.string();
    encryptionGlobalParams.encryptionCipherMode = "AES256-CBC";

    setGlobalServiceContext(ServiceContext::make());
    const ScopeGuard serviceContextCleanup = [&] { setGlobalServiceContext({}); };

    auto const service = getGlobalServiceContext();
    // Override the Noop encryption manager with the real encryption manager.
    setupEncryption<EncryptionKeyManager>(service, dbPathStr, &encryptionGlobalParams);

    auto encryptionManager = EncryptionKeyManager::get(service);

    // Make sure we can actually get some keys and that we've done some writes.
    auto systemKey = unittest::assertGet(encryptionManager->getKey(kSystemKeyId));
    auto tmpKey = unittest::assertGet(encryptionManager->getKey(kTmpDataKeyId));
    systemKey.reset();
    tmpKey.reset();

    // Get the list of files to backup. This will also mark the keystore as "dirty" which
    // would trigger a rollover if GCM were enabled.
    auto backupFiles = unittest::assertGet(encryptionManager->beginNonBlockingBackup());

    // Set up the encryption manager again so the old one gets destroyed and a new one
    // has to go through the initialization process.
    setupEncryption<EncryptionKeyManager>(service, dbPathStr, &encryptionGlobalParams);

    encryptionManager = EncryptionKeyManager::get(service);

    // Getting a key should work fine.
    systemKey = unittest::assertGet(encryptionManager->getKey(kSystemKeyId));
}

}  // namespace
}  // namespace mongo
