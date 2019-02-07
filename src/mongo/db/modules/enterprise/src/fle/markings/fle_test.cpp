/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"

#include "markings.h"

namespace mongo {
namespace {

// Positive: IsEncrypted is false
TEST(SchemaTests, IsEncrypted) {

    auto input = BSON("properties" << BSON("foo" << BSONObj()));

    ASSERT_FALSE(isEncryptionNeeded(input));
}

}  // namespace
}  // namespace mongo
