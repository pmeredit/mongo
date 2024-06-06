/**
 *    Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include <vector>

#include "encryption_key_manager.h"
#include "encryption_key_manager_noop.h"
#include "encryption_options.h"
#include "keystore.h"
#include "mongo/base/data_type_validated.h"
#include "mongo/bson/bson_validate.h"
#include "mongo/crypto/symmetric_key.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/remove_saver.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_extensions.h"
#include "mongo/rpc/object_check.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace {

class TestBufferStorage : public RemoveSaver::Storage {
public:
    TestBufferStorage(std::string* dump) : _dump{dump} {}

    std::unique_ptr<std::ostream> makeOstream(const boost::filesystem::path&,
                                              const boost::filesystem::path& root) override {
        auto temp = std::make_unique<std::ostringstream>();
        saved = temp.get();
        return temp;
    }

    void dumpBuffer() override {
        *_dump = saved->str();
    }

    std::ostringstream* saved{};
    std::string* _dump;
};

void setupEncryption(ServiceContext* service) {
    auto configHooks =
        std::make_unique<EncryptionWiredTigerCustomizationHooks>(&encryptionGlobalParams);
    WiredTigerCustomizationHooks::set(service, std::move(configHooks));
    auto keyManager = std::make_unique<EncryptionKeyManagerNoop>();
    EncryptionHooks::set(service, std::move(keyManager));
    WiredTigerExtensions::get(service)->addExtension(mongo::kEncryptionEntrypointConfig);
}

ServiceContext::ConstructorActionRegisterer registerEncryptionWiredTigerCustomizationHooks{
    "CreateEncryptionWiredTigerCustomizationHooks2",
    {"SetWiredTigerCustomizationHooks", "SecureAllocator"},
    [](ServiceContext* service) { setupEncryption(service); }};

void doTest(size_t i) {
    std::string buf;
    setGlobalServiceContext(ServiceContext::make());
    {
        auto testBuffer = std::make_unique<TestBufferStorage>(&buf);
        RemoveSaver removeSaver("RemoveSaverTest", "", "RemoveSaverTest", std::move(testBuffer));
        BSONObjBuilder bob;
        bob.append("", std::string(i, 'A'));
        ASSERT_OK(removeSaver.goingToDelete(bob.obj()));
    }
    ASSERT_TRUE(!buf.empty());
    std::vector<uint8_t> decrypted(buf.size());
    size_t decryptedLen;

    auto key = std::move(
        static_cast<EncryptionKeyManagerNoop*>(EncryptionHooks::get(getGlobalServiceContext()))
            ->getKey("test", EncryptionKeyManager::FindMode::kCurrent)
            .getValue());
    Status ret = aesDecrypt(*key,
                            crypto::aesMode::cbc,
                            crypto::PageSchema::k0,
                            reinterpret_cast<uint8_t*>(buf.data()) + 1,
                            buf.size() - 1,
                            decrypted.data(),
                            decrypted.size(),
                            &decryptedLen);
    ASSERT_OK(ret);
    ConstDataRangeCursor cursor(decrypted.data(), decryptedLen);
    while (!cursor.empty()) {
        // Here, we assert that the decrypted BSON Obj is valid
        const auto swObj = cursor.readAndAdvanceNoThrow<Validated<BSONObj>>();
        ASSERT_OK(swObj.getStatus());
    }
}

TEST(RemoveSaver, RemoveSaverPadding0) {
    doTest(0);
}

TEST(RemoveSaver, RemoveSaverPadding1) {
    doTest(1);
}

TEST(RemoveSaver, RemoveSaverPadding2) {
    doTest(2);
}

TEST(RemoveSaver, RemoveSaverPadding3) {
    doTest(3);
}

TEST(RemoveSaver, RemoveSaverPadding4) {
    doTest(4);
}

TEST(RemoveSaver, RemoveSaverPadding5) {
    doTest(5);
}

TEST(RemoveSaver, RemoveSaverPadding6) {
    doTest(6);
}

TEST(RemoveSaver, RemoveSaverPadding7) {
    doTest(7);
}

TEST(RemoveSaver, RemoveSaverPadding8) {
    doTest(8);
}

TEST(RemoveSaver, RemoveSaverPadding9) {
    doTest(9);
}

TEST(RemoveSaver, RemoveSaverPadding10) {
    doTest(10);
}

TEST(RemoveSaver, RemoveSaverPadding11) {
    doTest(11);
}

TEST(RemoveSaver, RemoveSaverPadding12) {
    doTest(12);
}

TEST(RemoveSaver, RemoveSaverPadding13) {
    doTest(13);
}

TEST(RemoveSaver, RemoveSaverPadding14) {
    doTest(14);
}

TEST(RemoveSaver, RemoveSaverPadding15) {
    doTest(15);
}

}  // namespace
}  // namespace mongo
