/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/service_context.h"
#include "mongo/util/exit_code.h"

namespace mongo {
ExitCode magicRestoreMain(ServiceContext* svcCtx);
}  // namespace mongo
