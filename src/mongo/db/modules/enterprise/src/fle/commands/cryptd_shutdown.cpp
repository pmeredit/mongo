/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/ntservice.h"

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

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& jsobj,
             BSONObjBuilder& result) final {
        ShutdownTaskArgs shutdownArgs;
        shutdownArgs.isUserInitiated = true;

        log() << "terminating, shutdown command received " << jsobj;

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
            shutdown(EXIT_CLEAN, shutdownArgs);  // this never returns
        }
    }
} cmdCryptDShutdown;

}  // namespace
}  // namespace mongo
