/**
 *  Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/base/init.h"
#include "mongo/base/status.h"

// This file allows to compile tests that require server options
// but do not depend on specific server libraries.

namespace mongo {
namespace {
MONGO_INITIALIZER_GENERAL(CoreOptions_Store,
                          ("BeginStartupOptionStorage"),
                          ("EndStartupOptionStorage"))
(InitializerContext* context) {}
}  // namespace
}  // namespace mongo
