/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/unittest.h"
#include "query_analysis.h"

namespace mongo {
namespace {

TEST(CommandsTest, IsEncryptedNotPresent) {
    auto input = BSON("properties" << BSON("foo" << BSONObj()));

    ASSERT_FALSE(isEncryptionNeeded(input));
}

TEST(CommandsTest, isEncryptionNeededEmptyEncrypt) {
    auto input = BSON("properties" << BSON("a" << BSON("encrypt" << BSONObj())) << "type"
                                   << "object");

    ASSERT_TRUE(isEncryptionNeeded(input));
}

TEST(CommandsTest, isEncryptionNeededDeepEncrypt) {
    auto input = BSON("properties" << BSON("a" << BSON("type"
                                                       << "object"
                                                       << "properties"
                                                       << BSON("b" << BSON("encrypt" << BSONObj())))
                                               << "c"
                                               << BSONObj())
                                   << "type"
                                   << "object");

    ASSERT_TRUE(isEncryptionNeeded(input));
}

}  // namespace
}  // namespace mongo
