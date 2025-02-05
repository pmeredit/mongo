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

#pragma once

#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
#include <climits>

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/fetcher.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/db/auth/validated_tenancy_scope.h"
#include "mongo/db/database_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/s/remove_shard_draining_progress_gen.h"
#include "mongo/db/server_parameter.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/logical_session_id.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/tenant_id.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/executor/task_executor.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/client/shard.h"

namespace mongo {
class ShardsvrAddShard;
class BSONObj;
class OperationContext;

class ShardId;

// Contains a collection of utility functions relating to the addShard command
namespace add_shard_util {

/*
 * The _id value for shard identity documents
 */
constexpr StringData kShardIdentityDocumentId = "shardIdentity"_sd;

/**
 * Creates an ShardsvrAddShard command object that's sent from the config server to
 * a mongod to instruct it to initialize itself as a shard in the cluster.
 */
ShardsvrAddShard createAddShardCmd(OperationContext* opCtx, const ShardId& shardName);

/**
 * Returns a BSON representation of an update request that can be used to insert a shardIdentity
 * doc into the shard with the given shardName (or update the shard's existing shardIdentity
 * doc's configsvrConnString if the _id, shardName, and clusterId do not conflict).
 */
BSONObj createShardIdentityUpsertForAddShard(const ShardsvrAddShard& addShardCmd,
                                             const WriteConcernOptions& wc);

}  // namespace add_shard_util

// TODO (SERVER-97816): remove these helpers and move the implementations into the add/remove shard
// coordinators once 9.0 becomes last LTS.
namespace topology_change_helpers {

// Returns the count of range deletion tasks locally on the config server.
long long getRangeDeletionCount(OperationContext* opCtx);

// Calls ShardsvrJoinMigrations locally on the config server.
void joinMigrations(OperationContext* opCtx);

/**
 * Used during addShard to determine if there is already an existing shard that matches the shard
 * that is currently being added. A boost::none indicates that there is no conflicting shard, and we
 * can proceed trying to add the new shard. A ShardType return indicates that there is an existing
 * shard that matches the shard being added but since the options match, this addShard request can
 * do nothing and return success. An exception indicates either a problem reading the existing
 * shards from disk or more likely indicates that an existing shard conflicts with the shard being
 * added and they have different options, so the addShard attempt must be aborted.
 */
boost::optional<ShardType> getExistingShard(OperationContext* opCtx,
                                            const ConnectionString& proposedShardConnectionString,
                                            const boost::optional<StringData>& proposedShardName,
                                            ShardingCatalogClient& localCatalogClient);

/**
 * Runs a command against a "shard" that is not yet in the cluster and thus not present in the
 * ShardRegistry.
 */
Shard::CommandResponse runCommandForAddShard(
    OperationContext* opCtx,
    RemoteCommandTargeter& targeter,
    const DatabaseName& dbName,
    const BSONObj& cmdObj,
    std::shared_ptr<executor::TaskExecutor> executorForAddShard);

enum UserWriteBlockingLevel {
    None = 0u,                     ///< Don't block anything
    DDLOperations = (1u << 0),     ///< Block DDLOperations
    Writes = (1u << 1),            ///< Block direct user writes
    All = DDLOperations | Writes,  ///< Block everything
};

/**
 * Sets the user write blocking state on a given target
 *
 * @param opCtx: The operation context
 * @param targeter: The targeter for the remote host target
 * @param level: The level of blocking. See `UserWriteBlockingLevel` for clarification
 * @param block: Flag to turn blocking on or off on the target for the given level. Eg: block ==
 *                  true && level == DDLOperations means blocking on DDL Operation (but not user
 *                  writes). block == false && level == All means unblock (allow) all user level
 *                  write operations.
 * @param osiGenerator: A generator function for operation session info. If exists, the generated
 *                  session info will be attached to the requests (one unique for each request).
 * @param executor: A task executor to run the requests on.
 */
void setUserWriteBlockingState(
    OperationContext* opCtx,
    RemoteCommandTargeter& targeter,
    UserWriteBlockingLevel level,
    bool block,
    boost::optional<std::function<OperationSessionInfo(OperationContext*)>> osiGenerator,
    std::shared_ptr<executor::TaskExecutor> executor);

std::vector<DatabaseName> getDBNamesListFromShard(OperationContext* opCtx,
                                                  RemoteCommandTargeter& targeter,
                                                  std::shared_ptr<executor::TaskExecutor> executor);

void removeReplicaSetMonitor(OperationContext* opCtx, const ConnectionString& connectionString);

BSONObj greetShard(OperationContext* opCtx,
                   RemoteCommandTargeter& targeter,
                   std::shared_ptr<executor::TaskExecutor> executorForAddShard);

void validateHostAsShard(OperationContext* opCtx,
                         RemoteCommandTargeter& targeter,
                         const ConnectionString& connectionString,
                         bool isConfigShard,
                         std::shared_ptr<executor::TaskExecutor> executor);

using FetcherDocsCallbackFn = std::function<bool(const std::vector<BSONObj>& batch)>;
using FetcherStatusCallbackFn = std::function<void(const Status& status)>;

/**
 * Creates a Fetcher task for fetching documents in the given collection on the given shard.
 * After the task is scheduled, applies 'processDocsCallback' to each fetched batch and
 * 'processStatusCallback' to the fetch status.
 */
std::unique_ptr<Fetcher> createFetcher(OperationContext* opCtx,
                                       RemoteCommandTargeter& targeter,
                                       const NamespaceString& nss,
                                       const repl::ReadConcernLevel& readConcernLevel,
                                       FetcherDocsCallbackFn processDocsCallback,
                                       FetcherStatusCallbackFn processStatusCallback,
                                       std::shared_ptr<executor::TaskExecutor> executor);

/**
 * Given the shard draining state, returns the message that should be included as part of the remove
 * shard response.
 */
std::string getRemoveShardMessage(const ShardDrainingStateEnum& status);

}  // namespace topology_change_helpers
}  // namespace mongo
