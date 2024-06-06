/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/logv2/log.h"
#include "mongo/util/exit.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/ntservice.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand


namespace mongo {
namespace {

/**
 * Implements { shutdown : 1} for mongocryptd
 *
 * NOTE: The only method called is run(), the rest exist simply to ensure the code compiles.
 */
class CryptDCmdShutdown : public BasicCommand {
public:
    CryptDCmdShutdown() : BasicCommand("shutdown") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kAlways;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const final {
        return false;
    }

    std::string help() const final {
        return "shutdown server";
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
        ShutdownTaskArgs shutdownArgs;
        shutdownArgs.isUserInitiated = true;

        LOGV2(24224, "terminating, shutdown command received {jsobj}", "jsobj"_attr = jsobj);

#if defined(_WIN32)
        // Signal the ServiceMain thread to shutdown.
        if (ntservice::shouldStartService()) {
            shutdownNoTerminate(shutdownArgs);

            // Client expects us to abruptly close the socket as part of exiting
            // so this function is not allowed to return.
            // The ServiceMain thread will quit for us so just sleep until it does.
            while (true)
                sleepsecs(60);  // Loop forever
        } else
#endif
        {
            shutdown(ExitCode::clean, shutdownArgs);  // this never returns
        }
    }
};
MONGO_REGISTER_COMMAND(CryptDCmdShutdown).forShard();

}  // namespace
}  // namespace mongo
