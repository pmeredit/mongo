/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#include <string.h>

#include "mongo/base/data_range.h"
#include "mongo/unittest/unittest.h"

#include "audit_key_manager.h"
#include "audit_key_manager_mock.h"

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

}  // namespace audit
}  // namespace mongo
