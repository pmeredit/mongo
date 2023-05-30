/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/db/commands.h"
#include "mongo/executor/connection_pool_stats.h"

#include "search_task_executors.h"

namespace mongo {

class CmdMongotConnPoolStats final : public BasicCommand {
public:
    CmdMongotConnPoolStats() : BasicCommand("_mongotConnPoolStats") {}

    std::string help() const override {
        return "internal testing command. Returns an object containing statistics about the "
               "connection pool between the server and mongot";
    }

    bool run(OperationContext* opCtx,
             const DatabaseName&,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        auto mongotExec = executor::getMongotTaskExecutor(opCtx->getServiceContext());
        executor::ConnectionPoolStats stats{};
        mongotExec->appendConnectionStats(&stats);
        stats.appendToBSON(result);
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    Status checkAuthForOperation(OperationContext*,
                                 const DatabaseName&,
                                 const BSONObj&) const override {
        return Status::OK();
    }
};

class CmdDropConnectionsToMongot final : public BasicCommand {
public:
    CmdDropConnectionsToMongot() : BasicCommand("_dropConnectionsToMongot") {}

    std::string help() const override {
        return "internal testing command. Used to drop connections between the server and mongot";
    }

    bool run(OperationContext* opCtx,
             const DatabaseName&,
             const BSONObj& cmdObj,
             BSONObjBuilder&) override {
        auto hps = cmdObj["hostAndPort"].Array();
        auto mongotExec = executor::getMongotTaskExecutor(opCtx->getServiceContext());
        for (auto&& hp : hps) {
            mongotExec->dropConnections(HostAndPort(hp.String()));
        }
        return true;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool adminOnly() const override {
        return true;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    Status checkAuthForOperation(OperationContext*,
                                 const DatabaseName&,
                                 const BSONObj&) const override {
        return Status::OK();
    }
};

MONGO_REGISTER_TEST_COMMAND(CmdMongotConnPoolStats);
MONGO_REGISTER_TEST_COMMAND(CmdDropConnectionsToMongot);

}  // namespace mongo
