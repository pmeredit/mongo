/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/wire_version.h"

namespace mongo {
namespace {

/**
 * Implements { isMaster : 1} for mongocryptd
 *
 * NOTE: The only method called is run(), the rest exist simply to ensure the code compiles.
 */
class CrytpDCmdIsMaster final : public BasicCommand {
public:
    CrytpDCmdIsMaster() : BasicCommand("isMaster", "ismaster") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kAlways;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        return false;
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
        result.appendBool("iscryptd", true);
        result.appendNumber("maxBsonObjectSize", BSONObjMaxUserSize);
        result.appendNumber("maxMessageSizeBytes", MaxMessageSizeBytes);
        result.appendDate("localTime", jsTime());

        // Mongos tries to keep exactly the same version range of the server for which
        // it is compiled.
        result.append("maxWireVersion", WireSpec::instance().incomingExternalClient.maxWireVersion);
        result.append("minWireVersion", WireSpec::instance().incomingExternalClient.minWireVersion);
        return true;
    }
} cmdCryptDIsMaster;

}  // namespace
}  // namespace mongo
