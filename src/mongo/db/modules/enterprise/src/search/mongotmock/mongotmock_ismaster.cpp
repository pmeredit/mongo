/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/wire_version.h"

namespace mongo {
namespace {

constexpr auto kHelloString = "hello"_sd;
// Aliases for the hello command in order to provide backwards compatibility.
constexpr auto kCamelCaseIsMasterString = "isMaster"_sd;
constexpr auto kLowerCaseIsMasterString = "ismaster"_sd;

/**
 * Implements { hello : 1} for mock_mongot.
 */
class MongotMockHello final : public BasicCommand {
public:
    MongotMockHello()
        : BasicCommand(kHelloString, {kCamelCaseIsMasterString, kLowerCaseIsMasterString}) {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        MONGO_UNREACHABLE;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        MONGO_UNREACHABLE;
    }

    std::string help() const final {
        return "Check if this server is primary for a replica set\n"
               "{ hello : 1 }";
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& jsobj,
             BSONObjBuilder& result) final {
        // Parse the command name, which should be one of the following: hello, isMaster, or
        // ismaster. If the command is "hello", we must attach an "isWritablePrimary" response field
        // instead of "ismaster".
        bool useLegacyResponseFields = (jsobj.firstElementFieldNameStringData() != kHelloString);

        if (useLegacyResponseFields) {
            result.appendBool("ismaster", true);
        } else {
            result.appendBool("isWritablePrimary", true);
        }

        result.appendBool("ismongot", true);
        result.appendNumber("maxBsonObjectSize", BSONObjMaxUserSize);
        result.appendNumber("maxMessageSizeBytes", static_cast<long long>(MaxMessageSizeBytes));
        result.appendDate("localTime", jsTime());

        auto wireSpec = WireSpec::instance().get();
        result.append("maxWireVersion", wireSpec->incomingExternalClient.maxWireVersion);
        result.append("minWireVersion", wireSpec->incomingExternalClient.minWireVersion);

        // The mongod paired with a mongotmock should be able to auth as the __system user with
        // the SCRAM-SHA-1 authentication mechanism.
        result.append("saslSupportedMechs", BSON_ARRAY("SCRAM-SHA-1"));
        return true;
    }
} cmdMongotMockHello;

}  // namespace
}  // namespace mongo
