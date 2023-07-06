/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include <string>

#include "mongo/base/init.h"  // IWYU pragma: keep
#include "mongo/base/initializer.h"

namespace mongo {
namespace {
MONGO_INITIALIZER_GENERAL(CoreOptions_Store,
                          ("BeginStartupOptionStorage"),
                          ("EndStartupOptionStorage"))
(InitializerContext* context) {}
}  // namespace
}  // namespace mongo
