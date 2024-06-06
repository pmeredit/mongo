/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "version_mongoqd.h"

#include <iostream>

#include "mongo/db/log_process_details.h"
#include "mongo/db/server_options.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_domain_global.h"
#include "mongo/logv2/log_manager.h"
#include "mongo/platform/process_id.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/version.h"
#include "serverless/version_mongoqd.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding


namespace mongo {
namespace {

std::string mongoqdVersion(const VersionInfoInterface& provider) {
    return formatVersionString("mongoqd", provider);
}

}  // namespace

void logMongoqdVersionInfo(std::ostream* os) {
    if (os) {
        auto&& vii = VersionInfoInterface::instance();
        *os << mongoqdVersion(vii) << std::endl;
        vii.logBuildInfo(os);
        *os << std::endl;
    } else {
        logProcessDetails(nullptr);
    }
}

}  // namespace mongo
