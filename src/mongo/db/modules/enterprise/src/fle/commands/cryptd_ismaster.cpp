/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/wire_version.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {
namespace {

constexpr auto kHelloString = "hello"_sd;
// Aliases for the hello command in order to provide backwards compatibility.
constexpr auto kCamelCaseIsMasterString = "isMaster"_sd;
constexpr auto kLowerCaseIsMasterString = "ismaster"_sd;

/**
 * Implements { hello : 1} for mongocryptd
 *
 * NOTE: The only method called is run(), the rest exist simply to ensure the code compiles.
 */
class CrytpDCmdHello final : public BasicCommand {
public:
    CrytpDCmdHello()
        : BasicCommand(kHelloString, {kCamelCaseIsMasterString, kLowerCaseIsMasterString}) {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kAlways;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        return false;
    }

    std::string help() const final {
        return "Check if this server is primary for a replica set\n"
               "{ hello : 1 }";
    }

    Status checkAuthForOperation(OperationContext*,
                                 const DatabaseName&,
                                 const BSONObj&) const final {
        return {ErrorCodes::Unauthorized, "unauthorized"};
    }

    bool run(OperationContext* opCtx,
             const DatabaseName&,
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

        result.appendBool("iscryptd", true);
        result.appendNumber("maxBsonObjectSize", BSONObjMaxUserSize);
        result.appendNumber("maxMessageSizeBytes", static_cast<long long>(MaxMessageSizeBytes));
        result.appendDate("localTime", jsTime());

        // Mongos tries to keep exactly the same version range of the server for which
        // it is compiled.
        auto wireSpec = WireSpec::getWireSpec(opCtx->getServiceContext()).get();
        result.append("maxWireVersion", wireSpec->incomingExternalClient.maxWireVersion);
        result.append("minWireVersion", wireSpec->incomingExternalClient.minWireVersion);
        return true;
    }
};
MONGO_REGISTER_COMMAND(CrytpDCmdHello).forShard();

}  // namespace
}  // namespace mongo
