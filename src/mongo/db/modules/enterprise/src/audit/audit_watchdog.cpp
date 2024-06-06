/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit_manager.h"

#include <boost/filesystem.hpp>

#include "mongo/base/init.h"
#include "mongo/watchdog/watchdog_register.h"

namespace mongo {

MONGO_INITIALIZER_WITH_PREREQUISITES(RegisterAuditWatchdog,
                                     ("EndStartupOptionHandling", "InitializeGlobalAuditManager"))
(::mongo::InitializerContext* context) {

    const auto& path = audit::getGlobalAuditManager()->getPath();
    if (!path.empty()) {
        boost::filesystem::path auditFile(path);
        auto auditPath = auditFile.parent_path();
        registerWatchdogPath(auditPath.generic_string());
    }
}

}  // namespace mongo
