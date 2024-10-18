/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/db/commands/buildinfo_common.h"

namespace mongo {
namespace {
MONGO_REGISTER_COMMAND(CmdBuildInfoCommon).forShard();
}  // namespace
}  // namespace mongo
