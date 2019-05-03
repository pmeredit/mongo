/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/util/version.h"

namespace mongo {
namespace {

/**
 * Implements { buildInfo : 1} for mock mongocryptd.
 */
class MongotMockBuildInfo : public BasicCommand {
public:
    MongotMockBuildInfo() : BasicCommand("buildInfo", "buildinfo") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        MONGO_UNREACHABLE;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        return "get version #, etc.\n"
               "{ buildinfo:1 }";
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& jsobj,
             BSONObjBuilder& result) final {
        VersionInfoInterface::instance().appendBuildInfo(&result);
        result.append("mongotmock", true);
        return true;
    }
} cmdMongotMockBuildInfo;

}  // namespace
}  // namespace mongo
