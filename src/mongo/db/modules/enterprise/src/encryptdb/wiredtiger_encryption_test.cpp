/**
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <string>

#include "encryption_key_manager_noop.h"
#include "encryption_options.h"
#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_recovery_unit.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace {
ServiceContext::ConstructorActionRegisterer createEncryptionKeyManager{
    "CreateEncryptionKeyManager",
    {"CreateKeyEntropySource", "SecureAllocator", "SetWiredTigerCustomizationHooks"},
    [](ServiceContext* service) {
        // Setup the custom hooks required to enable encryption
        auto configHooks = stdx::make_unique<WiredTigerCustomizationHooks>();
        WiredTigerCustomizationHooks::set(service, std::move(configHooks));
        auto keyManager = stdx::make_unique<EncryptionKeyManagerNoop>();
        EncryptionHooks::set(service, std::move(keyManager));
        encryptionGlobalParams.enableEncryption = true;
    }};

class WiredTigerConnection {
public:
    WiredTigerConnection(StringData dbpath, StringData cipherName) : _conn(NULL) {
        encryptionGlobalParams.encryptionCipherMode = cipherName.toString();
        std::stringstream ss;
        ss << "create,cache_size=100MB,";
        ss << "extensions=[local=(entry=mongo_addWiredTigerEncryptors)],";
        ss << "encryption=(name=" << cipherName << ",keyid=system),";
        std::string config = ss.str();
        int ret = wiredtiger_open(dbpath.toString().c_str(), NULL, config.c_str(), &_conn);

        ASSERT_OK(wtRCToStatus(ret));
        ASSERT(_conn);
    }
    ~WiredTigerConnection() {
        _conn->close(_conn, NULL);
    }
    WT_CONNECTION* getConnection() const {
        return _conn;
    }

private:
    WT_CONNECTION* _conn;
};

class WiredTigerUtilHarnessHelper {
public:
    WiredTigerUtilHarnessHelper(const std::string& dbPath, const std::string& cipherName)
        : _cipherName(cipherName),
          _connection(dbPath, cipherName),
          _sessionCache(_connection.getConnection()) {}

    WiredTigerSessionCache* getSessionCache() {
        return &_sessionCache;
    }

    OperationContext* newOperationContext() {
        return new OperationContextNoop(
            new WiredTigerRecoveryUnit(getSessionCache(), &_oplogManager));
    }

    void writeData() {
        WiredTigerRecoveryUnit* ru = new WiredTigerRecoveryUnit(&_sessionCache, &_oplogManager);
        OperationContextNoop opCtx(ru);
        WiredTigerSession* mongoSession = ru->getSession();

        WriteUnitOfWork uow(&opCtx);
        WT_SESSION* session = mongoSession->getSession();

        /*
        * Create and open some encrypted and not encrypted tables.
        */
        const std::string encryptionConfigPrefix = std::string("encryption=(name=") + _cipherName;
        ASSERT_OK(
            wtRCToStatus(session->create(session,
                                         "table:crypto1",
                                         (encryptionConfigPrefix + ",keyid=abc),"
                                                                   "columns=(key0,value0),"
                                                                   "key_format=S,value_format=S")
                                             .c_str())));
        ASSERT_OK(
            wtRCToStatus(session->create(session,
                                         "table:crypto2",
                                         (encryptionConfigPrefix + ",keyid=efg),"
                                                                   "columns=(key0,value0),"
                                                                   "key_format=S,value_format=S")
                                             .c_str())));
        WT_CURSOR *c1, *c2;
        ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto1", NULL, NULL, &c1)));
        ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto2", NULL, NULL, &c2)));

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

    void readData() {
        WiredTigerRecoveryUnit recoveryUnit(&_sessionCache, &_oplogManager);
        WiredTigerSession* mongoSession = recoveryUnit.getSession();
        WT_SESSION* session = mongoSession->getSession();

        WT_CURSOR *c1, *c2;
        ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto1", NULL, NULL, &c1)));
        ASSERT_OK(wtRCToStatus(session->open_cursor(session, "table:crypto2", NULL, NULL, &c2)));

        char *key1, *val1, *key2, *val2;
        int ret;
        while (c1->next(c1) == 0) {
            ret = c2->next(c2);
            ret = c1->get_key(c1, &key1);
            ret = c1->get_value(c1, &val1);
            ret = c2->get_key(c2, &key2);
            ret = c2->get_value(c2, &val2);

            ASSERT(strcmp(key1, key2) == 0) << "Key1 " << key1 << " and Key2 " << key2
                                            << " do not match";
            ASSERT(strcmp(val1, val2) == 0) << "Val1 " << val1 << " and Val2 " << val2
                                            << " do not match";
        }
    }


private:
    std::string _cipherName;
    WiredTigerConnection _connection;
    WiredTigerSessionCache _sessionCache;
    WiredTigerOplogManager _oplogManager;
};

TEST_F(ServiceContextTest, ReadWriteDataCBC) {
    unittest::TempDir dbPath("cbc_wt_test");
    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256CBCName);
        helper.writeData();
    }
    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256CBCName);
        helper.readData();
    }
}


#if !defined(DISABLE_GCM_TESTVECTORS)
TEST_F(ServiceContextTest, ReadWriteDataGCM) {
    if (crypto::getSupportedSymmetricAlgorithms().count(crypto::aes256GCMName) == 0) {
        return;
    }

    unittest::TempDir dbPath("gcm_wt_test");
    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256GCMName);
        helper.writeData();
    }
    {
        WiredTigerUtilHarnessHelper helper(dbPath.path(), crypto::aes256GCMName);
        helper.readData();
    }
}
#endif

}  // namespace
}  // namespace mongo
