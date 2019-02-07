/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/util/version.h"

namespace mongo {
namespace {

/**
 * Implements { buildInfo : 1} for mongocryptd
 *
 * NOTE: The only method called is run(), the rest exist simply to ensure the code compiles.
 */
class CryptDCmdBuildInfo : public BasicCommand {
public:
    CryptDCmdBuildInfo() : BasicCommand("buildInfo", "buildinfo") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kAlways;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        return false;
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
        return true;
    }
};

MONGO_REGISTER_TEST_COMMAND(CryptDCmdBuildInfo);

}  // namespace
}  // namespace mongo
