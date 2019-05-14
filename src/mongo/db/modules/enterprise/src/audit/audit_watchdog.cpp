/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "audit_options.h"

#include <boost/filesystem.hpp>

#include "mongo/base/init.h"
#include "mongo/watchdog/watchdog_register.h"

namespace mongo {

MONGO_INITIALIZER_WITH_PREREQUISITES(RegisterAuditWatchdog, ("EndStartupOptionHandling"))
(::mongo::InitializerContext* context) {

    if (!audit::auditGlobalParams.auditPath.empty()) {
        boost::filesystem::path auditFile(audit::auditGlobalParams.auditPath);
        auto auditPath = auditFile.parent_path();
        registerWatchdogPath(auditPath.generic_string());
    }

    return Status::OK();
}

}  // namespace mongo
