/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <cstring>

#include "audit_key_manager.h"
#include "audit_key_manager_local.h"
#include "audit_key_manager_mock.h"
#include "mongo/base/data_range.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace audit {

class AuditKeyManagerMockTest : public unittest::Test {
public:
    AuditKeyManagerMock mock;
};

TEST_F(AuditKeyManagerMockTest, KeysRoundTrip) {
    auto [originalKey, wrappedKey] = mock.generateWrappedKey();

    SymmetricKey unwrappedKey = mock.unwrapKey(wrappedKey);
    ASSERT_EQ(originalKey.getKeySize(), unwrappedKey.getKeySize());
    ASSERT_EQ(0, memcmp(originalKey.getKey(), unwrappedKey.getKey(), originalKey.getKeySize()));
}

TEST_F(AuditKeyManagerMockTest, ModifiedKeysDoNotRoundTrip) {
    auto result = mock.generateWrappedKey();

    result.wrappedKey.push_back(0xFF);

    ASSERT_THROWS_CODE(mock.unwrapKey(result.wrappedKey), DBException, ErrorCodes::OperationFailed);
}

class AuditKeyManagerLocalTest : public unittest::Test {
public:
    AuditKeyManagerLocalTest();
    std::unique_ptr<AuditKeyManagerLocal> keyMgr;
    std::unique_ptr<unittest::TempDir> keyTempDir;
    boost::filesystem::path keyFilePath;
};

AuditKeyManagerLocalTest::AuditKeyManagerLocalTest() {
    // create temp dir to contain the test key file
    keyTempDir = std::make_unique<unittest::TempDir>("auditLocalKeystore");
    boost::filesystem::path dbPath(keyTempDir->path());

    // make sure the path is absolute
    if (!dbPath.is_absolute()) {
        dbPath = boost::filesystem::absolute(dbPath);
    }
    ASSERT_TRUE(boost::filesystem::exists(dbPath));

    // create the key file
    keyFilePath = dbPath / "keyFile";
    {
        std::ofstream keyPathOut(keyFilePath.c_str());
        keyPathOut << "LxbM6ik6lgJjKEkugTdkRUoHCeyArNxg2xx7kGHl/io=";
    }

#ifndef _WIN32
    // Make sure the permissions are correct.
    boost::filesystem::permissions(keyFilePath, boost::filesystem::owner_read);
#endif
}

TEST_F(AuditKeyManagerLocalTest, KeysRoundTrip) {
    keyMgr = std::make_unique<AuditKeyManagerLocal>(keyFilePath.string());
    auto [originalKey, wrappedKey] = keyMgr->generateWrappedKey();
    ASSERT_NE(originalKey.getKeySize(), wrappedKey.size());

    SymmetricKey unwrappedKey = keyMgr->unwrapKey(wrappedKey);
    ASSERT_EQ(originalKey.getKeySize(), unwrappedKey.getKeySize());
    ASSERT_EQ(0, memcmp(originalKey.getKey(), unwrappedKey.getKey(), originalKey.getKeySize()));
}

TEST_F(AuditKeyManagerLocalTest, ModifiedKeysDoNotRoundTrip) {
    keyMgr = std::make_unique<AuditKeyManagerLocal>(keyFilePath.string());
    auto [originalKey, wrappedKey] = keyMgr->generateWrappedKey();

    wrappedKey.front() ^= 1;

    auto unwrappedKey = keyMgr->unwrapKey(wrappedKey);
    ASSERT_EQ(originalKey.getKeySize(), unwrappedKey.getKeySize());
    ASSERT_NE(0, memcmp(originalKey.getKey(), unwrappedKey.getKey(), originalKey.getKeySize()));
}

}  // namespace audit
}  // namespace mongo
