/**
 *    Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <memory>
#include <string>

#include "encryption_key_manager_noop.h"
#include "encryption_options.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/crypto/symmetric_crypto.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_connection.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_recovery_unit.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/system_clock_source.h"

namespace mongo {
namespace {

ServiceContext::ConstructorActionRegisterer createEncryptionKeyManager{
    "CreateEncryptionKeyManager",
    {"SecureAllocator", "SetWiredTigerCustomizationHooks"},
    [](ServiceContext* service) {
        // Setup the custom hooks required to enable encryption
        auto configHooks = std::make_unique<WiredTigerCustomizationHooks>();
        WiredTigerCustomizationHooks::set(service, std::move(configHooks));
        auto keyManager = std::make_unique<EncryptionKeyManagerNoop>();
        EncryptionHooks::set(service, std::move(keyManager));
        encryptionGlobalParams.enableEncryption = true;
    }};

class WiredTigerConnectionTest {
public:
    WiredTigerConnectionTest(StringData dbpath, StringData cipherName) : _conn(nullptr) {
        encryptionGlobalParams.encryptionCipherMode = cipherName.toString();
        std::stringstream ss;
        ss << "create,cache_size=100MB,";
        ss << "extensions=[local=(entry=mongo_addWiredTigerEncryptors)],";
        ss << "encryption=(name=" << cipherName << ",keyid=system),";
        std::string config = ss.str();
        _fastClockSource = std::make_unique<SystemClockSource>();
        int ret = wiredtiger_open(dbpath.toString().c_str(), nullptr, config.c_str(), &_conn);

        ASSERT_OK(wtRCToStatus(ret, nullptr));
        ASSERT(_conn);
    }
    ~WiredTigerConnectionTest() {
        _conn->close(_conn, nullptr);
    }
    WT_CONNECTION* getConnection() const {
        return _conn;
    }
    ClockSource* getClockSource() {
        return _fastClockSource.get();
    }

private:
    WT_CONNECTION* _conn;
    std::unique_ptr<ClockSource> _fastClockSource;
};

class WiredTigerUtilHarnessHelper {
public:
    WiredTigerUtilHarnessHelper(const std::string& dbPath, const std::string& cipherName)
        : _cipherName(cipherName),
          _connectionTest(dbPath, cipherName),
          _connection(_connectionTest.getConnection(), _connectionTest.getClockSource()) {}

    void writeData(OperationContext* opCtx) {
        WiredTigerRecoveryUnit ru = WiredTigerRecoveryUnit(&_connection, nullptr);
        ru.setOperationContext(opCtx);
        WiredTigerSession* mongoSession = ru.getSession();

        WriteUnitOfWork uow(opCtx);
        WT_SESSION* session = mongoSession->getSession();

        /*
         * Create and open some encrypted and not encrypted tables.
         */
        const std::string encryptionConfigPrefix = std::string("encryption=(name=") + _cipherName;
        ASSERT_OK(wtRCToStatus(session->create(session,
                                               "table:crypto1",
                                               (encryptionConfigPrefix +
                                                ",keyid=abc),"
                                                "columns=(key0,value0),"
                                                "key_format=S,value_format=S")
                                                   .c_str()),
                               session));
        ASSERT_OK(wtRCToStatus(session->create(session,
                                               "table:crypto2",
                                               (encryptionConfigPrefix +
                                                ",keyid=efg),"
                                                "columns=(key0,value0),"
                                                "key_format=S,value_format=S")
                                                   .c_str()),
                               session));
        WT_CURSOR *c1, *c2;
        ASSERT_OK(wtRCToStatus(
            session->open_cursor(session, "table:crypto1", nullptr, nullptr, &c1), session));
        ASSERT_OK(wtRCToStatus(
            session->open_cursor(session, "table:crypto2", nullptr, nullptr, &c2), session));

        /*
         * Insert a set of keys and values.  Insert the same data into
         * all tables so that we can verify they're all the same after
         * we decrypt on read.
         */
        for (int i = 0; i < 10; i++) {
            std::stringstream keyBuf;
            keyBuf << "key" << i;
            std::string key = keyBuf.str();
            c1->set_key(c1, key.c_str());
            c2->set_key(c2, key.c_str());

            std::stringstream valueBuf;
            keyBuf << "value" << i;
            std::string val = valueBuf.str();
            c1->set_value(c1, val.c_str());
            c2->set_value(c2, val.c_str());

            c1->insert(c1);
            c2->insert(c2);
        }

        uow.commit();
    }

    void readData(OperationContext* opCtx) {
        WiredTigerRecoveryUnit recoveryUnit(&_connection, nullptr);
        recoveryUnit.setOperationContext(opCtx);
        WiredTigerSession* mongoSession = recoveryUnit.getSession();
        WT_SESSION* session = mongoSession->getSession();

        WT_CURSOR *c1, *c2;
        ASSERT_OK(wtRCToStatus(
            session->open_cursor(session, "table:crypto1", nullptr, nullptr, &c1), session));
        ASSERT_OK(wtRCToStatus(
            session->open_cursor(session, "table:crypto2", nullptr, nullptr, &c2), session));

        char *key1, *val1, *key2, *val2;
        while (c1->next(c1) == 0) {
            ASSERT_EQ(0, c2->next(c2));
            ASSERT_EQ(0, c1->get_key(c1, &key1));
            ASSERT_EQ(0, c1->get_value(c1, &val1));
            ASSERT_EQ(0, c2->get_key(c2, &key2));
            ASSERT_EQ(0, c2->get_value(c2, &val2));

            ASSERT(strcmp(key1, key2) == 0)
                << "Key1 " << key1 << " and Key2 " << key2 << " do not match";
            ASSERT(strcmp(val1, val2) == 0)
                << "Val1 " << val1 << " and Val2 " << val2 << " do not match";
        }
    }

private:
    std::string _cipherName;
    WiredTigerConnectionTest _connectionTest;
    WiredTigerConnection _connection;
};

class WiredTigerEncryptionTest : public ServiceContextMongoDTest {};

TEST_F(WiredTigerEncryptionTest, ReadWriteDataCBC) {
    unittest::TempDir dbPath("cbc_wt_test");
    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256CBCName);
        auto opCtx = makeOperationContext();
        helper.writeData(opCtx.get());
    }
    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256CBCName);
        auto opCtx = makeOperationContext();
        helper.readData(opCtx.get());
    }
}

#if !defined(DISABLE_GCM_TESTVECTORS)
TEST_F(WiredTigerEncryptionTest, ReadWriteDataGCM) {
    if (crypto::getSupportedSymmetricAlgorithms().count(crypto::aes256GCMName) == 0) {
        return;
    }

    unittest::TempDir dbPath("gcm_wt_test");

    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256GCMName);
        auto opCtx = makeOperationContext();
        helper.writeData(opCtx.get());
    }
    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256GCMName);
        auto opCtx = makeOperationContext();
        helper.readData(opCtx.get());
    }
}
#endif

}  // namespace
}  // namespace mongo
