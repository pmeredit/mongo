/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/wire_version.h"

namespace mongo {
namespace {

/**
 * Implements { isMaster : 1} for mock_mongot.
 */
class MongotMockIsMaster final : public BasicCommand {
public:
    MongotMockIsMaster() : BasicCommand("isMaster", "ismaster") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        MONGO_UNREACHABLE;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        return "Check if this server is primary for a replica set\n"
               "{ isMaster : 1 }";
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& jsobj,
             BSONObjBuilder& result) final {
        result.appendBool("ismaster", true);
        result.appendBool("ismongot", true);
        result.appendNumber("maxBsonObjectSize", BSONObjMaxUserSize);
        result.appendNumber("maxMessageSizeBytes", MaxMessageSizeBytes);
        result.appendDate("localTime", jsTime());

        result.append("maxWireVersion", WireSpec::instance().incomingExternalClient.maxWireVersion);
        result.append("minWireVersion", WireSpec::instance().incomingExternalClient.minWireVersion);
        return true;
    }
} cmdMongotMockIsMaster;

}  // namespace
}  // namespace mongo
