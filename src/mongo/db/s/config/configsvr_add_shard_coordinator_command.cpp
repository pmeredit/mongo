/**
 *    Copyright (C) 2025-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include <functional>
#include <string>

#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/client/connection_string.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/client.h"
#include "mongo/db/cluster_role.h"
#include "mongo/db/commands.h"
#include "mongo/db/database_name.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/add_shard_coordinator.h"
#include "mongo/db/s/add_shard_coordinator_document_gen.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameter.h"
#include "mongo/db/service_context.h"
#include "mongo/db/tenant_id.h"
#include "mongo/logv2/log.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/request_types/add_shard_gen.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

namespace mongo {

/**
 * TODO (SERVER-99284)
 * This is a test command for internal testing of SPM-4017 - Resilient Topology Changes
 * DO NOT USE THIS COMMAND!
 */
class ConfigSvrAddShardCoordinatorCommand
    : public TypedCommand<ConfigSvrAddShardCoordinatorCommand> {
public:
    using Request = ConfigsvrAddShardCoordinator;
    using Response = AddShardResponse;

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        Response typedRun(OperationContext* opCtx) {
            uassert(ErrorCodes::IllegalOperation,
                    "_configsvrAddShard can only be run on config servers",
                    serverGlobalParams.clusterRole.has(ClusterRole::ConfigServer));
            CommandHelpers::uassertCommandRunWithMajority(Request::kCommandName,
                                                          opCtx->getWriteConcern());

            // Set the operation context read concern level to local for reads into the config
            // database.
            repl::ReadConcernArgs::get(opCtx) =
                repl::ReadConcernArgs(repl::ReadConcernLevel::kLocalReadConcern);

            const auto target = request().getCommandParameter();
            const auto name = request().getName()
                ? boost::make_optional(request().getName()->toString())
                : boost::none;

            auto replCoord = repl::ReplicationCoordinator::get(opCtx);
            auto validationStatus = _validate(target, replCoord->getConfig().isLocalHostAllowed());
            uassertStatusOK(validationStatus);

            audit::logAddShard(Client::getCurrent(), name ? name.value() : "", target.toString());

            const auto addShardCoordinator =
                checked_pointer_cast<AddShardCoordinator>(std::invoke([&]() {
                    auto coordinatorDoc = AddShardCoordinatorDocument();
                    coordinatorDoc.setConnectionString(target);
                    coordinatorDoc.setIsConfigShard(false);
                    coordinatorDoc.setProposedName(name);
                    coordinatorDoc.setShardingDDLCoordinatorMetadata(
                        {{NamespaceString::kConfigsvrShardsNamespace,
                          DDLCoordinatorTypeEnum::kAddShard}});

                    return ShardingDDLCoordinatorService::getService(opCtx)->getOrCreateInstance(
                        opCtx, coordinatorDoc.toBSON());
                }));

            const auto finalName = addShardCoordinator->getResult(opCtx);

            Response result;
            result.setShardAdded(finalName);

            return result;
        }

    private:
        bool supportsWriteConcern() const override {
            return true;
        }

        NamespaceString ns() const override {
            return {};
        }

        void doCheckAuthorization(OperationContext* opCtx) const override {
            uassert(ErrorCodes::Unauthorized,
                    "Unauthorized",
                    AuthorizationSession::get(opCtx->getClient())
                        ->isAuthorizedForActionsOnResource(
                            ResourcePattern::forClusterResource(request().getDbName().tenantId()),
                            ActionType::internal));
        }

        static Status _validate(const ConnectionString& target, bool allowLocalHost) {
            // Check that if one of the new shard's hosts is localhost, we are allowed to use
            // localhost as a hostname. (Using localhost requires that every server in the cluster
            // uses localhost).
            for (const auto& serverAddr : target.getServers()) {
                if (serverAddr.isLocalHost() != allowLocalHost) {
                    std::string errmsg = str::stream()
                        << "Can't use localhost as a shard since all shards need to"
                        << " communicate. Either use all shards and configdbs in localhost"
                        << " or all in actual IPs. host: " << serverAddr.toString()
                        << " isLocalHost:" << serverAddr.isLocalHost();
                    return Status(ErrorCodes::InvalidOptions, errmsg);
                }
            }
            return Status::OK();
        }
    };

    bool skipApiVersionCheck() const override {
        // Internal command (server to server).
        return true;
    }

    std::string help() const override {
        return "Internal command, which is exported by the sharding config server. Do not call "
               "directly. Validates and adds a new shard to the cluster.";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }
};
MONGO_REGISTER_COMMAND(ConfigSvrAddShardCoordinatorCommand).forShard();

}  // namespace mongo
