/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#include <boost/cstdint.hpp>
#include <boost/move/utility_core.hpp>
#include <boost/none.hpp>
#include <boost/optional.hpp>
#include <boost/optional/optional.hpp>
#include <boost/smart_ptr.hpp>
#include <fmt/format.h>
// IWYU pragma: no_include "ext/alloc_traits.h"
#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <istream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bson_field.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobj_comparator.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/bson/timestamp.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/fetcher.h"
#include "mongo/client/read_preference.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/db/audit.h"
#include "mongo/db/catalog/drop_database.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/cluster_server_parameter_cmds_gen.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/commands/notify_sharding_event_gen.h"
#include "mongo/db/commands/set_cluster_parameter_invocation.h"
#include "mongo/db/commands/set_feature_compatibility_version_gen.h"
#include "mongo/db/commands/set_user_write_block_mode_gen.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/database_name.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/feature_flag.h"
#include "mongo/db/generic_argument_util.h"
#include "mongo/db/keys_collection_document_gen.h"
#include "mongo/db/keys_collection_util.h"
#include "mongo/db/list_collections_gen.h"
#include "mongo/db/logical_time.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/persistent_task_store.h"
#include "mongo/db/query/find_command.h"
#include "mongo/db/query/write_ops/write_ops_gen.h"
#include "mongo/db/query/write_ops/write_ops_parsers.h"
#include "mongo/db/read_write_concern_defaults.h"
#include "mongo/db/repl/hello_gen.h"
#include "mongo/db/repl/optime_with.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/read_concern_level.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/wait_for_majority_service.h"
#include "mongo/db/resource_yielder.h"
#include "mongo/db/s/config/sharding_catalog_manager.h"
#include "mongo/db/s/range_deletion_task_gen.h"
#include "mongo/db/s/remove_shard_draining_progress_gen.h"
#include "mongo/db/s/replica_set_endpoint_feature_flag.h"
#include "mongo/db/s/sharding_config_server_parameters_gen.h"
#include "mongo/db/s/sharding_ddl_util.h"
#include "mongo/db/s/sharding_logging.h"
#include "mongo/db/s/sharding_util.h"
#include "mongo/db/s/topology_change_helpers.h"
#include "mongo/db/s/user_writes_critical_section_document_gen.h"
#include "mongo/db/s/user_writes_recoverable_critical_section_service.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameter.h"
#include "mongo/db/service_context.h"
#include "mongo/db/session/logical_session_id.h"
#include "mongo/db/shard_id.h"
#include "mongo/db/tenant_id.h"
#include "mongo/db/transaction/transaction_api.h"
#include "mongo/db/vector_clock_mutable.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/executor/connection_pool_stats.h"
#include "mongo/executor/inline_executor.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/remote_command_response.h"
#include "mongo/executor/task_executor.h"
#include "mongo/executor/task_executor_pool.h"
#include "mongo/idl/cluster_server_parameter_common.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/rpc/metadata.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_database_gen.h"
#include "mongo/s/catalog/type_namespace_placement_gen.h"
#include "mongo/s/catalog/type_remove_shard_event_gen.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/database_version.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/add_shard_gen.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"
#include "mongo/s/request_types/shardsvr_join_migrations_request_gen.h"
#include "mongo/s/sharding_cluster_parameters_gen.h"
#include "mongo/s/sharding_feature_flags_gen.h"
#include "mongo/s/sharding_state.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/clock_source.h"
#include "mongo/util/database_name_util.h"
#include "mongo/util/decorable.h"
#include "mongo/util/duration.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/future.h"
#include "mongo/util/future_impl.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/out_of_line_executor.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"
#include "mongo/util/uuid.h"
#include "mongo/util/version/releases.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding


namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(hangAddShardBeforeUpdatingClusterCardinalityParameter);
MONGO_FAIL_POINT_DEFINE(hangRemoveShardAfterSettingDrainingFlag);
MONGO_FAIL_POINT_DEFINE(hangRemoveShardAfterDrainingDDL);
MONGO_FAIL_POINT_DEFINE(hangRemoveShardBeforeUpdatingClusterCardinalityParameter);
MONGO_FAIL_POINT_DEFINE(skipUpdatingClusterCardinalityParameterAfterAddShard);
MONGO_FAIL_POINT_DEFINE(skipUpdatingClusterCardinalityParameterAfterRemoveShard);
MONGO_FAIL_POINT_DEFINE(changeBSONObjMaxUserSize);

using CallbackHandle = executor::TaskExecutor::CallbackHandle;
using CallbackArgs = executor::TaskExecutor::CallbackArgs;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using RemoteCommandCallbackFn = executor::TaskExecutor::RemoteCommandCallbackFn;

const ReadPreferenceSetting kConfigReadSelector(ReadPreference::Nearest, TagSet{});
const WriteConcernOptions kMajorityWriteConcern{WriteConcernOptions::kMajority,
                                                WriteConcernOptions::SyncMode::UNSET,
                                                WriteConcernOptions::kNoTimeout};

const Seconds kRemoteCommandTimeout{60};

constexpr StringData kAddOrRemoveShardInProgressRecoveryDocumentId =
    "addOrRemoveShardInProgressRecovery"_sd;

/**
 * Generates a unique name to be given to a newly added shard.
 */
StatusWith<std::string> generateNewShardName(OperationContext* opCtx, Shard* configShard) {
    BSONObjBuilder shardNameRegex;
    shardNameRegex.appendRegex(ShardType::name(), "^shard");

    auto findStatus =
        configShard->exhaustiveFindOnConfig(opCtx,
                                            kConfigReadSelector,
                                            repl::ReadConcernLevel::kLocalReadConcern,
                                            NamespaceString::kConfigsvrShardsNamespace,
                                            shardNameRegex.obj(),
                                            BSON(ShardType::name() << -1),
                                            1);
    if (!findStatus.isOK()) {
        return findStatus.getStatus();
    }

    const auto& docs = findStatus.getValue().docs;

    int count = 0;
    if (!docs.empty()) {
        const auto shardStatus = ShardType::fromBSON(docs.front());
        if (!shardStatus.isOK()) {
            return shardStatus.getStatus();
        }

        std::istringstream is(shardStatus.getValue().getName().substr(5));
        is >> count;
        count++;
    }

    // TODO: fix so that we can have more than 10000 automatically generated shard names
    if (count < 9999) {
        std::stringstream ss;
        ss << "shard" << std::setfill('0') << std::setw(4) << count;
        return ss.str();
    }

    return Status(ErrorCodes::OperationFailed, "unable to generate new shard name");
}

// If an add/removeShard recovery document is present on kServerConfigurationNamespace, unset the
// addOrRemoveShardInProgress cluster parameter. Must be called under the _kAddRemoveShardLock lock.
void resetDDLBlockingForTopologyChangeIfNeeded(OperationContext* opCtx) {
    // Check if we need to run recovery at all.
    {
        DBDirectClient client(opCtx);
        const auto recoveryDoc =
            client.findOne(NamespaceString::kServerConfigurationNamespace,
                           BSON("_id" << kAddOrRemoveShardInProgressRecoveryDocumentId));
        if (recoveryDoc.isEmpty()) {
            // No need to do anything.
            return;
        }
    }

    // Unset the addOrRemoveShardInProgress cluster parameter.
    LOGV2(5687906, "Resetting addOrRemoveShardInProgress cluster parameter after failure");
    topology_change_helpers::unblockDDLCoordinators(opCtx);
    LOGV2(5687907, "Resetted addOrRemoveShardInProgress cluster parameter after failure");
}

AggregateCommandRequest makeUnshardedCollectionsOnSpecificShardAggregation(OperationContext* opCtx,
                                                                           const ShardId& shardId,
                                                                           bool isCount = false) {
    BSONObj listStage = BSON("$listClusterCatalog" << BSON("shards" << true));
    BSONObj matchStage =
        BSON("$match" << BSON("$and" << BSON_ARRAY(BSON("sharded" << false << "shards" << shardId)
                                                   << BSON("db" << BSON("$ne"
                                                                        << "config"))
                                                   << BSON("db" << BSON("$ne"
                                                                        << "admin")))));
    BSONObj countStage = BSON("$count"
                              << "totalCount");

    auto dbName = NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kAdmin);

    std::vector<mongo::BSONObj> pipeline;
    pipeline.reserve(isCount ? 3 : 2);
    pipeline.push_back(listStage);
    pipeline.push_back(matchStage);
    if (isCount) {
        pipeline.push_back(countStage);
    }

    AggregateCommandRequest aggRequest{dbName, pipeline};
    aggRequest.setReadConcern(repl::ReadConcernArgs::kLocal);
    aggRequest.setWriteConcern({});
    return aggRequest;
}

std::vector<NamespaceString> getCollectionsToMoveForShard(OperationContext* opCtx,
                                                          Shard* shard,
                                                          const ShardId& shardId) {

    auto listCollectionAggReq = makeUnshardedCollectionsOnSpecificShardAggregation(opCtx, shardId);

    std::vector<NamespaceString> collections;

    uassertStatusOK(
        shard->runAggregation(opCtx,
                              listCollectionAggReq,
                              [&collections](const std::vector<BSONObj>& batch,
                                             const boost::optional<BSONObj>& postBatchResumeToken) {
                                  for (const auto& doc : batch) {
                                      collections.push_back(NamespaceStringUtil::deserialize(
                                          boost::none,
                                          doc.getField("ns").String(),
                                          SerializationContext::stateDefault()));
                                  }
                                  return true;
                              }));

    return collections;
}

bool appendToArrayIfRoom(int offset,
                         BSONArrayBuilder& arrayBuilder,
                         const std::string& toAppend,
                         const int maxUserSize) {
    if (static_cast<int>(offset + arrayBuilder.len() + toAppend.length()) < maxUserSize) {
        arrayBuilder.append(toAppend);
        return true;
    }
    return false;
}

}  // namespace

StatusWith<Shard::CommandResponse> ShardingCatalogManager::_runCommandForAddShard(
    OperationContext* opCtx,
    RemoteCommandTargeter* targeter,
    const DatabaseName& dbName,
    const BSONObj& cmdObj) {
    try {
        return topology_change_helpers::runCommandForAddShard(
            opCtx, *targeter, dbName, cmdObj, _executorForAddShard);
    } catch (const DBException& ex) {
        return ex.toStatus();
    }
}

StatusWith<ShardType> ShardingCatalogManager::_validateHostAsShard(
    OperationContext* opCtx,
    std::shared_ptr<RemoteCommandTargeter> targeter,
    const std::string* shardProposedName,
    const ConnectionString& connectionString,
    bool isConfigShard) {

    try {
        topology_change_helpers::validateHostAsShard(
            opCtx, *targeter, connectionString, isConfigShard, _executorForAddShard);
    } catch (DBException& ex) {
        return ex.toStatus();
    }

    auto resHelloStatus = std::invoke([&]() -> StatusWith<BSONObj> {
        try {
            return topology_change_helpers::greetShard(opCtx, *targeter, _executorForAddShard);
        } catch (DBException& ex) {
            return ex.toStatus();
        }
    });
    if (!resHelloStatus.isOK()) {
        return resHelloStatus.getStatus();
    }

    auto resHello = std::move(resHelloStatus.getValue());
    const std::string foundSetName = resHello["setName"].str();

    std::string actualShardName;

    if (shardProposedName) {
        actualShardName = *shardProposedName;
    } else if (!foundSetName.empty()) {
        // Default it to the name of the replica set
        actualShardName = foundSetName;
    }

    // Disallow adding shard replica set with name 'config'
    if (!isConfigShard && actualShardName == DatabaseName::kConfig.db(omitTenant)) {
        return {ErrorCodes::BadValue, "use of shard replica set with name 'config' is not allowed"};
    }

    // Retrieve the most up to date connection string that we know from the replica set monitor (if
    // this is a replica set shard, otherwise it will be the same value as connectionString).
    ConnectionString actualShardConnStr = targeter->connectionString();

    ShardType shard;
    shard.setName(actualShardName);
    shard.setHost(actualShardConnStr.toString());
    shard.setState(ShardType::ShardState::kShardAware);

    return shard;
}

Status ShardingCatalogManager::_dropSessionsCollection(
    OperationContext* opCtx, std::shared_ptr<RemoteCommandTargeter> targeter) {

    BSONObjBuilder builder;
    builder.append("drop", NamespaceString::kLogicalSessionsNamespace.coll());
    {
        BSONObjBuilder wcBuilder(builder.subobjStart("writeConcern"));
        wcBuilder.append("w", "majority");
    }

    auto swCommandResponse = _runCommandForAddShard(
        opCtx, targeter.get(), NamespaceString::kLogicalSessionsNamespace.dbName(), builder.done());
    if (!swCommandResponse.isOK()) {
        return swCommandResponse.getStatus();
    }

    auto cmdStatus = std::move(swCommandResponse.getValue().commandStatus);
    if (!cmdStatus.isOK() && cmdStatus.code() != ErrorCodes::NamespaceNotFound) {
        return cmdStatus;
    }

    return Status::OK();
}

StatusWith<std::vector<DatabaseName>> ShardingCatalogManager::_getDBNamesListFromShard(
    OperationContext* opCtx, std::shared_ptr<RemoteCommandTargeter> targeter) {
    try {
        return topology_change_helpers::getDBNamesListFromShard(
            opCtx, *targeter, _executorForAddShard);
    } catch (DBException& ex) {
        return ex.toStatus();
    }
}

StatusWith<std::vector<CollectionType>> ShardingCatalogManager::_getCollListFromShard(
    OperationContext* opCtx,
    const std::vector<DatabaseName>& dbNames,
    std::shared_ptr<RemoteCommandTargeter> targeter) {
    std::vector<CollectionType> nssList;

    for (auto& dbName : dbNames) {
        Status fetchStatus =
            Status(ErrorCodes::InternalError, "Internal error running cursor callback in command");
        auto host = uassertStatusOK(
            targeter->findHost(opCtx, ReadPreferenceSetting{ReadPreference::PrimaryOnly}));
        const Milliseconds maxTimeMS =
            std::min(opCtx->getRemainingMaxTimeMillis(), Milliseconds(kRemoteCommandTimeout));

        auto fetcherCallback = [&](const Fetcher::QueryResponseStatus& dataStatus,
                                   Fetcher::NextAction* nextAction,
                                   BSONObjBuilder* getMoreBob) {
            // Throw out any accumulated results on error.
            if (!dataStatus.isOK()) {
                fetchStatus = dataStatus.getStatus();
                return;
            }
            const auto& data = dataStatus.getValue();

            try {
                for (const BSONObj& doc : data.documents) {
                    auto collInfo = ListCollectionsReplyItem::parse(
                        IDLParserContext("ListCollectionReply"), doc);
                    // Skip views and special collections.
                    if (!collInfo.getInfo() || !collInfo.getInfo()->getUuid()) {
                        continue;
                    }

                    const auto nss = NamespaceStringUtil::deserialize(dbName, collInfo.getName());

                    if (nss.isNamespaceAlwaysUntracked()) {
                        continue;
                    }

                    auto coll = CollectionType(nss,
                                               OID::gen(),
                                               Timestamp(Date_t::now()),
                                               Date_t::now(),
                                               collInfo.getInfo()->getUuid().get(),
                                               sharding_ddl_util::unsplittableCollectionShardKey());
                    coll.setUnsplittable(true);
                    if (!doc["options"].eoo() && !doc["options"]["timeseries"].eoo()) {
                        coll.setTimeseriesFields(TypeCollectionTimeseriesFields::parse(
                            IDLParserContext("AddShardContext"),
                            doc["options"]["timeseries"].Obj()));
                    }
                    nssList.push_back(coll);
                }
                *nextAction = Fetcher::NextAction::kNoAction;
            } catch (DBException& ex) {
                fetchStatus = ex.toStatus();
                return;
            }
            fetchStatus = Status::OK();

            if (!getMoreBob) {
                return;
            }
            getMoreBob->append("getMore", data.cursorId);
            getMoreBob->append("collection", data.nss.coll());
        };
        ListCollections listCollections;
        listCollections.setDbName(dbName);
        auto fetcher =
            std::make_unique<Fetcher>(_executorForAddShard.get(),
                                      host,
                                      dbName,
                                      listCollections.toBSON(),
                                      fetcherCallback,
                                      BSONObj() /* metadata tracking, only used for shards */,
                                      maxTimeMS /* command network timeout */,
                                      maxTimeMS /* getMore network timeout */);

        auto scheduleStatus = fetcher->schedule();
        if (!scheduleStatus.isOK()) {
            return scheduleStatus;
        }

        auto joinStatus = fetcher->join(opCtx);
        if (!joinStatus.isOK()) {
            return joinStatus;
        }
        if (!fetchStatus.isOK()) {
            return fetchStatus;
        }
    }

    return nssList;
}

void ShardingCatalogManager::installConfigShardIdentityDocument(OperationContext* opCtx) {
    invariant(!ShardingState::get(opCtx)->enabled());

    // Insert a shard identity document. Note we insert with local write concern, so the shard
    // identity may roll back, which will trigger an fassert to clear the in-memory sharding state.
    {
        auto addShardCmd = add_shard_util::createAddShardCmd(opCtx, ShardId::kConfigServerId);

        auto shardIdUpsertCmd = add_shard_util::createShardIdentityUpsertForAddShard(
            addShardCmd, ShardingCatalogClient::kLocalWriteConcern);

        // A request dispatched through a local client is served within the same thread that submits
        // it (so that the opCtx needs to be used as the vehicle to pass the WC to the
        // ServiceEntryPoint).
        const auto originalWC = opCtx->getWriteConcern();
        ScopeGuard resetWCGuard([&] { opCtx->setWriteConcern(originalWC); });
        opCtx->setWriteConcern(ShardingCatalogClient::kLocalWriteConcern);

        DBDirectClient localClient(opCtx);
        BSONObj res;

        localClient.runCommand(DatabaseName::kAdmin, shardIdUpsertCmd, res);

        uassertStatusOK(getStatusFromWriteCommandReply(res));
    }
}

Status ShardingCatalogManager::_updateClusterCardinalityParameter(const Lock::ExclusiveLock&,
                                                                  OperationContext* opCtx,
                                                                  int numShards) {
    ConfigsvrSetClusterParameter configsvrSetClusterParameter(BSON(
        "shardedClusterCardinalityForDirectConns"
        << BSON(ShardedClusterCardinalityParam::kHasTwoOrMoreShardsFieldName << (numShards >= 2))));
    configsvrSetClusterParameter.setDbName(DatabaseName::kAdmin);

    const auto shardRegistry = Grid::get(opCtx)->shardRegistry();

    while (true) {
        const auto cmdResponse = shardRegistry->getConfigShard()->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting(ReadPreference::PrimaryOnly),
            DatabaseName::kAdmin,
            configsvrSetClusterParameter.toBSON(),
            Shard::RetryPolicy::kIdempotent);

        auto status = Shard::CommandResponse::getEffectiveStatus(cmdResponse);

        if (status != ErrorCodes::ConflictingOperationInProgress) {
            return status;
        }

        // Retry on ErrorCodes::ConflictingOperationInProgress errors, which can be caused by
        // another ConfigsvrCoordinator runnning concurrently.
        LOGV2_DEBUG(9314400,
                    2,
                    "Failed to update the cluster parameter. Retrying again after 500ms.",
                    "error"_attr = status);

        opCtx->sleepFor(Milliseconds(500));
    }
}

Status ShardingCatalogManager::_updateClusterCardinalityParameterAfterAddShardIfNeeded(
    const Lock::ExclusiveLock& clusterCardinalityParameterLock, OperationContext* opCtx) {
    if (MONGO_unlikely(skipUpdatingClusterCardinalityParameterAfterAddShard.shouldFail())) {
        return Status::OK();
    }

    auto numShards = Grid::get(opCtx)->shardRegistry()->getNumShards(opCtx);
    if (numShards == 2) {
        // Only need to update the parameter when adding the second shard.
        return _updateClusterCardinalityParameter(
            clusterCardinalityParameterLock, opCtx, numShards);
    }
    return Status::OK();
}

Status ShardingCatalogManager::_updateClusterCardinalityParameterAfterRemoveShardIfNeeded(
    const Lock::ExclusiveLock& clusterCardinalityParameterLock, OperationContext* opCtx) {
    if (MONGO_unlikely(skipUpdatingClusterCardinalityParameterAfterRemoveShard.shouldFail())) {
        return Status::OK();
    }

    // If the replica set endpoint is not active, then it isn't safe to allow direct connections
    // again after a second shard has been added. Unsharded collections are allowed to be tracked
    // and moved as soon as a second shard is added to the cluster, and these collections will not
    // handle direct connections properly.
    if (!replica_set_endpoint::isFeatureFlagEnabled()) {
        return Status::OK();
    }

    auto numShards = Grid::get(opCtx)->shardRegistry()->getNumShards(opCtx);
    if (numShards == 1) {
        // Only need to update the parameter when removing the second shard.
        return _updateClusterCardinalityParameter(
            clusterCardinalityParameterLock, opCtx, numShards);
    }
    return Status::OK();
}

Status ShardingCatalogManager::updateClusterCardinalityParameterIfNeeded(OperationContext* opCtx) {
    Lock::ExclusiveLock clusterCardinalityParameterLock(opCtx, _kClusterCardinalityParameterLock);

    auto shardRegistry = Grid::get(opCtx)->shardRegistry();
    shardRegistry->reload(opCtx);

    auto numShards = shardRegistry->getNumShards(opCtx);
    if (numShards <= 2) {
        // Only need to update the parameter when adding or removing the second shard.
        return _updateClusterCardinalityParameter(
            clusterCardinalityParameterLock, opCtx, numShards);
    }
    return Status::OK();
}

StatusWith<std::string> ShardingCatalogManager::addShard(
    OperationContext* opCtx,
    const std::string* shardProposedName,
    const ConnectionString& shardConnectionString,
    bool isConfigShard) {
    if (!shardConnectionString) {
        return {ErrorCodes::BadValue, "Invalid connection string"};
    }

    if (shardProposedName && shardProposedName->empty()) {
        return {ErrorCodes::BadValue, "shard name cannot be empty"};
    }

    const auto shardRegistry = Grid::get(opCtx)->shardRegistry();

    Lock::ExclusiveLock addRemoveShardLock(opCtx, _kAddRemoveShardLock);

    // Unset the addOrRemoveShardInProgress cluster parameter in case it was left set by a previous
    // failed addShard/removeShard operation.
    resetDDLBlockingForTopologyChangeIfNeeded(opCtx);

    // Take the cluster cardinality parameter lock and the shard membership lock in exclusive mode
    // so that no add/remove shard operation and its set cluster cardinality parameter operation can
    // interleave with the ones below. Release the shard membership lock before initiating the
    // _configsvrSetClusterParameter command after finishing the add shard operation since setting a
    // cluster parameter requires taking this lock.
    Lock::ExclusiveLock clusterCardinalityParameterLock(opCtx, _kClusterCardinalityParameterLock);
    Lock::ExclusiveLock shardMembershipLock(opCtx, _kShardMembershipLock);

    // Check if this shard has already been added (can happen in the case of a retry after a network
    // error, for example) and thus this addShard request should be considered a no-op.
    const auto existingShard = std::invoke([&]() -> StatusWith<boost::optional<ShardType>> {
        try {
            return topology_change_helpers::getExistingShard(
                opCtx,
                shardConnectionString,
                shardProposedName ? boost::optional<StringData>(*shardProposedName) : boost::none,
                *_localCatalogClient);
        } catch (const DBException& ex) {
            return ex.toStatus();
        }
    });
    if (!existingShard.isOK()) {
        return existingShard.getStatus();
    }
    if (existingShard.getValue()) {
        // These hosts already belong to an existing shard, so report success and terminate the
        // addShard request.  Make sure to set the last optime for the client to the system last
        // optime so that we'll still wait for replication so that this state is visible in the
        // committed snapshot.
        repl::ReplClientInfo::forClient(opCtx->getClient()).setLastOpToSystemLastOpTime(opCtx);

        // Release the shard membership lock since the set cluster parameter operation below
        // require taking this lock.
        shardMembershipLock.unlock();
        auto updateStatus = _updateClusterCardinalityParameterAfterAddShardIfNeeded(
            clusterCardinalityParameterLock, opCtx);
        if (!updateStatus.isOK()) {
            return updateStatus;
        }

        return existingShard.getValue()->getName();
    }

    shardMembershipLock.unlock();
    clusterCardinalityParameterLock.unlock();

    const std::shared_ptr<Shard> shard{shardRegistry->createConnection(shardConnectionString)};
    auto targeter = shard->getTargeter();

    ScopeGuard stopMonitoringGuard(
        [&] { topology_change_helpers::removeReplicaSetMonitor(opCtx, shardConnectionString); });

    // Validate the specified connection string may serve as shard at all
    auto shardStatus = _validateHostAsShard(
        opCtx, targeter, shardProposedName, shardConnectionString, isConfigShard);
    if (!shardStatus.isOK()) {
        return shardStatus.getStatus();
    }
    ShardType& shardType = shardStatus.getValue();

    // TODO SERVER-80532: the sharding catalog might lose some databases.
    // Check that none of the existing shard candidate's dbs exist already
    auto dbNamesStatus = _getDBNamesListFromShard(opCtx, targeter);
    if (!dbNamesStatus.isOK()) {
        return dbNamesStatus.getStatus();
    }

    for (const auto& dbName : dbNamesStatus.getValue()) {
        try {
            auto dbt = _localCatalogClient->getDatabase(
                opCtx, dbName, repl::ReadConcernLevel::kLocalReadConcern);
            return Status(ErrorCodes::OperationFailed,
                          str::stream()
                              << "can't add shard "
                              << "'" << shardConnectionString.toString() << "'"
                              << " because a local database '" << dbName.toStringForErrorMsg()
                              << "' exists in another " << dbt.getPrimary());
        } catch (const ExceptionFor<ErrorCodes::NamespaceNotFound>&) {
        }
    }

    // Check that the shard candidate does not have a local config.system.sessions collection. We do
    // not want to drop this once featureFlagSessionsCollectionCoordinatorOnConfigServer is enabled
    // but we do not have stability yet. We optimistically do not drop it here and then double check
    // later under the fixed FCV region.
    if (!isConfigShard) {
        auto res = _dropSessionsCollection(opCtx, targeter);
        if (!res.isOK()) {
            return res.withContext(
                "can't add shard with a local copy of config.system.sessions, please drop this "
                "collection from the shard manually and try again.");
        }

        // If the shard is also the config server itself, there is no need to pull the keys since
        // the keys already exists in the local admin.system.keys collection.
        auto pullKeysStatus = _pullClusterTimeKeys(opCtx, targeter);
        if (!pullKeysStatus.isOK()) {
            return pullKeysStatus;
        }
    }

    // If a name for a shard wasn't provided, generate one
    if (shardType.getName().empty()) {
        auto result = generateNewShardName(opCtx, _localConfigShard.get());
        if (!result.isOK()) {
            return result.getStatus();
        }
        shardType.setName(result.getValue());
    }

    // Helper function that runs a command on the to-be shard and returns the status
    auto runCmdOnNewShard = [this, &opCtx, &targeter](const BSONObj& cmd) -> Status {
        auto swCommandResponse =
            _runCommandForAddShard(opCtx, targeter.get(), DatabaseName::kAdmin, cmd);
        if (!swCommandResponse.isOK()) {
            return swCommandResponse.getStatus();
        }
        // Grabs the underlying status from a StatusWith object by taking the first
        // non-OK status, if there is one. This is needed due to the semantics of
        // _runCommandForAddShard.
        auto commandResponse = std::move(swCommandResponse.getValue());
        BatchedCommandResponse batchResponse;
        return Shard::CommandResponse::processBatchWriteResponse(commandResponse, &batchResponse);
    };

    if (!isConfigShard) {
        ShardsvrAddShard addShardCmd =
            add_shard_util::createAddShardCmd(opCtx, shardType.getName());

        // Use the _addShard command to add the shard, which in turn inserts a shardIdentity
        // document into the shard and triggers sharding state initialization.
        auto addShardStatus = runCmdOnNewShard(addShardCmd.toBSON());
        if (!addShardStatus.isOK()) {
            return addShardStatus;
        }

        // Set the user-writes blocking state on the new shard.
        _setUserWriteBlockingStateOnNewShard(opCtx, targeter.get());

        // Determine the set of cluster parameters to be used.
        _standardizeClusterParameters(opCtx, shard.get());
    }

    {
        // Keep the FCV stable across checking the FCV, sending setFCV to the new shard and writing
        // the entry for the new shard to config.shards. This ensures the FCV doesn't change after
        // we send setFCV to the new shard, but before we write its entry to config.shards.
        //
        // NOTE: We don't use a Global IX lock here, because we don't want to hold the global lock
        // while blocking on the network).
        FixedFCVRegion fcvRegion(opCtx);

        const auto fcvSnapshot = (*fcvRegion).acquireFCVSnapshot();

        std::vector<CollectionType> collList;
        if (feature_flags::gTrackUnshardedCollectionsUponCreation.isEnabled(fcvSnapshot)) {
            // TODO SERVER-80532: the sharding catalog might lose some collections.
            auto listStatus = _getCollListFromShard(opCtx, dbNamesStatus.getValue(), targeter);
            if (!listStatus.isOK()) {
                return listStatus.getStatus();
            }

            collList = std::move(listStatus.getValue());
        }

        // (Generic FCV reference): These FCV checks should exist across LTS binary versions.
        uassert(5563603,
                "Cannot add shard while in upgrading/downgrading FCV state",
                !fcvSnapshot.isUpgradingOrDowngrading());

        const auto currentFCV = fcvSnapshot.getVersion();
        invariant(currentFCV == multiversion::GenericFCV::kLatest ||
                  currentFCV == multiversion::GenericFCV::kLastContinuous ||
                  currentFCV == multiversion::GenericFCV::kLastLTS);

        if (isConfigShard &&
            !feature_flags::gSessionsCollectionCoordinatorOnConfigServer.isEnabled(fcvSnapshot)) {
            auto res = _dropSessionsCollection(opCtx, targeter);
            if (!res.isOK()) {
                return res.withContext(
                    "can't add shard with a local copy of config.system.sessions, please drop this "
                    "collection from the shard manually and try again.");
            }
        }

        if (!isConfigShard) {
            SetFeatureCompatibilityVersion setFcvCmd(currentFCV);
            setFcvCmd.setDbName(DatabaseName::kAdmin);
            setFcvCmd.setFromConfigServer(true);
            setFcvCmd.setWriteConcern(opCtx->getWriteConcern());

            auto versionResponse = _runCommandForAddShard(
                opCtx, targeter.get(), DatabaseName::kAdmin, setFcvCmd.toBSON());
            if (!versionResponse.isOK()) {
                return versionResponse.getStatus();
            }

            if (!versionResponse.getValue().commandStatus.isOK()) {
                return versionResponse.getValue().commandStatus;
            }
        }

        // Block new ShardingDDLCoordinators on the cluster and join ongoing ones.
        ScopeGuard unblockDDLCoordinatorsGuard([&] { scheduleAsyncUnblockDDLCoordinators(opCtx); });
        if (feature_flags::gStopDDLCoordinatorsDuringTopologyChanges.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            topology_change_helpers::blockDDLCoordinatorsAndDrain(opCtx);
        }

        // Tick clusterTime to get a new topologyTime for this mutation of the topology.
        auto newTopologyTime = VectorClockMutable::get(opCtx)->tickClusterTime(1);

        shardType.setTopologyTime(newTopologyTime.asTimestamp());

        LOGV2(21942,
              "Going to insert new entry for shard into config.shards",
              "shardType"_attr = shardType.toString());

        clusterCardinalityParameterLock.lock();
        shardMembershipLock.lock();

        {
            // Execute the transaction with a local write concern to make sure `stopMonitorGuard` is
            // dimissed only when the transaction really fails.
            const auto originalWC = opCtx->getWriteConcern();
            ScopeGuard resetWCGuard([&] { opCtx->setWriteConcern(originalWC); });
            opCtx->setWriteConcern(ShardingCatalogClient::kLocalWriteConcern);

            _addShardInTransaction(
                opCtx, shardType, std::move(dbNamesStatus.getValue()), std::move(collList));
        }
        // Once the transaction has committed, we must immediately dismiss the guard to avoid
        // incorrectly removing the RSM after persisting the shard addition.
        stopMonitoringGuard.dismiss();

        // Wait for majority can only be done after dismissing the `stopMonitoringGuard`.
        if (opCtx->getWriteConcern().isMajority()) {
            WaitForMajorityService::get(opCtx->getServiceContext())
                .waitUntilMajorityForWrite(
                    repl::ReplicationCoordinator::get(opCtx->getServiceContext())
                        ->getMyLastAppliedOpTime(),
                    opCtx->getCancellationToken())
                .get();
        }

        // Record in changelog
        BSONObjBuilder shardDetails;
        shardDetails.append("name", shardType.getName());
        shardDetails.append("host", shardConnectionString.toString());

        ShardingLogging::get(opCtx)->logChange(opCtx,
                                               "addShard",
                                               NamespaceString::kEmpty,
                                               shardDetails.obj(),
                                               ShardingCatalogClient::kMajorityWriteConcern,
                                               _localConfigShard,
                                               _localCatalogClient.get());

        // Ensure the added shard is visible to this process.
        shardRegistry->reload(opCtx);
        tassert(9870600,
                "Shard not found in ShardRegistry after committing addShard",
                shardRegistry->getShard(opCtx, shardType.getName()).isOK());

        hangAddShardBeforeUpdatingClusterCardinalityParameter.pauseWhileSet(opCtx);
        // Release the shard membership lock since the set cluster parameter operation below
        // require taking this lock.
        shardMembershipLock.unlock();

        // Unblock ShardingDDLCoordinators on the cluster.
        if (feature_flags::gStopDDLCoordinatorsDuringTopologyChanges.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            // TODO SERVER-99708: Investigate if we can remove this. unblockDDLCoordinators ends up
            // calling the configSvrSetClusterParameter command which takes the FCV lock. At this
            // point we are still holding the clusterCardinalityParameterLock. Other operations
            // proceed to take the locks in the inverse order, that is, they first take the FCV lock
            // followed by the clusterCardinalityParameterLock. We should review if we must unlock
            // the clusterCardinalityParameterLock here to prevent a lock cycle or see if this can
            // be refactored to prevent it.
            DisableLockerRuntimeOrderingChecks disableChecks{opCtx};
            topology_change_helpers::unblockDDLCoordinators(opCtx);
        }
        unblockDDLCoordinatorsGuard.dismiss();

        auto updateStatus = _updateClusterCardinalityParameterAfterAddShardIfNeeded(
            clusterCardinalityParameterLock, opCtx);
        if (!updateStatus.isOK()) {
            return updateStatus;
        }

        return shardType.getName();
    }
}

void ShardingCatalogManager::addConfigShard(OperationContext* opCtx) {
    // Set the operation context read concern level to local for reads into the config
    // database.
    repl::ReadConcernArgs::get(opCtx) =
        repl::ReadConcernArgs(repl::ReadConcernLevel::kLocalReadConcern);

    auto configConnString = repl::ReplicationCoordinator::get(opCtx)->getConfigConnectionString();

    auto shardingState = ShardingState::get(opCtx);
    uassert(7368500, "sharding state not enabled", shardingState->enabled());

    std::string shardName = shardingState->shardId().toString();
    uassertStatusOK(addShard(opCtx, &shardName, configConnString, true));
}


RemoveShardProgress ShardingCatalogManager::removeShard(OperationContext* opCtx,
                                                        const ShardId& shardId) {
    Lock::ExclusiveLock addRemoveShardLock(opCtx, _kAddRemoveShardLock);

    // Unset the addOrRemoveShardInProgress cluster parameter in case it was left set by a previous
    // failed addShard/removeShard operation.
    resetDDLBlockingForTopologyChangeIfNeeded(opCtx);

    const auto shardName = shardId.toString();
    audit::logRemoveShard(opCtx->getClient(), shardName);

    // Take the cluster cardinality parameter lock and the shard membership lock in exclusive mode
    // so that no add/remove shard operation and its set cluster cardinality parameter operation can
    // interleave with the ones below. Release the shard membership lock before initiating the
    // _configsvrSetClusterParameter command after finishing the remove shard operation since
    // setting a cluster parameter requires taking this lock.
    Lock::ExclusiveLock clusterCardinalityParameterLock(opCtx, _kClusterCardinalityParameterLock);
    Lock::ExclusiveLock shardMembershipLock =
        ShardingCatalogManager::acquireShardMembershipLockForTopologyChange(opCtx);

    auto findShardResponse = uassertStatusOK(
        _localConfigShard->exhaustiveFindOnConfig(opCtx,
                                                  kConfigReadSelector,
                                                  repl::ReadConcernLevel::kLocalReadConcern,
                                                  NamespaceString::kConfigsvrShardsNamespace,
                                                  BSON(ShardType::name() << shardName),
                                                  BSONObj(),
                                                  1));

    if (findShardResponse.docs.empty()) {
        // Release the shard membership lock since the set cluster parameter operation below
        // requires taking this lock.
        shardMembershipLock.unlock();

        auto updateStatus = _updateClusterCardinalityParameterAfterRemoveShardIfNeeded(
            clusterCardinalityParameterLock, opCtx);
        uassertStatusOK(updateStatus);
        uasserted(ErrorCodes::ShardNotFound,
                  str::stream() << "Shard " << shardId << " does not exist");
    }

    const auto shard = uassertStatusOK(ShardType::fromBSON(findShardResponse.docs[0]));
    const auto replicaSetName =
        uassertStatusOK(ConnectionString::parse(shard.getHost())).getReplicaSetName();

    // Find how many *other* shards exist, which are *not* currently draining
    const auto countOtherNotDrainingShards = topology_change_helpers::runCountCommandOnConfig(
        opCtx,
        _localConfigShard,
        NamespaceString::kConfigsvrShardsNamespace,
        BSON(ShardType::name() << NE << shardName << ShardType::draining.ne(true)));
    uassert(ErrorCodes::IllegalOperation,
            "Operation not allowed because it would remove the last shard",
            countOtherNotDrainingShards > 0);

    // Ensure there are no non-empty zones that only belong to this shard
    for (auto& zoneName : shard.getTags()) {
        auto isRequiredByZone = uassertStatusOK(
            _isShardRequiredByZoneStillInUse(opCtx, kConfigReadSelector, shardName, zoneName));
        uassert(ErrorCodes::ZoneStillInUse,
                str::stream()
                    << "Operation not allowed because it would remove the only shard for zone "
                    << zoneName << " which has a chunk range is associated with it",
                !isRequiredByZone);
    }

    // Figure out if shard is already draining
    const bool isShardCurrentlyDraining =
        topology_change_helpers::runCountCommandOnConfig(
            opCtx,
            _localConfigShard,
            NamespaceString::kConfigsvrShardsNamespace,
            BSON(ShardType::name() << shardName << ShardType::draining(true))) > 0;

    if (!isShardCurrentlyDraining) {
        LOGV2(21945, "Going to start draining shard", "shardId"_attr = shardName);

        // Record start in changelog
        uassertStatusOK(
            ShardingLogging::get(opCtx)->logChangeChecked(opCtx,
                                                          "removeShard.start",
                                                          NamespaceString::kEmpty,
                                                          BSON("shard" << shardName),
                                                          ShardingCatalogClient::kLocalWriteConcern,
                                                          _localConfigShard,
                                                          _localCatalogClient.get()));

        uassertStatusOKWithContext(_localCatalogClient->updateConfigDocument(
                                       opCtx,
                                       NamespaceString::kConfigsvrShardsNamespace,
                                       BSON(ShardType::name() << shardName),
                                       BSON("$set" << BSON(ShardType::draining(true))),
                                       false,
                                       ShardingCatalogClient::kLocalWriteConcern),
                                   "error starting removeShard");

        return {ShardDrainingStateEnum::kStarted};
    }

    shardMembershipLock.unlock();
    clusterCardinalityParameterLock.unlock();

    hangRemoveShardAfterSettingDrainingFlag.pauseWhileSet(opCtx);

    // Draining has already started, now figure out how many chunks and databases are still on the
    // shard.
    auto drainingProgress =
        topology_change_helpers::getDrainingProgress(opCtx, _localConfigShard, shardName);
    // The counters: `shardedChunks`, `totalCollections`, and `databases` are used to present the
    // ongoing status to the user. Additionally, `totalChunks` on the shard is checked for safety,
    // as it is a critical point in the removeShard process, to ensure that a non-empty shard is not
    // removed. For example the number of unsharded collections might be inaccurate due to
    // $listClusterCatalog potentially returning an incorrect list of shards during concurrent DDL
    // operations.
    if (!drainingProgress.isFullyDrained()) {
        // Still more draining to do
        LOGV2(21946,
              "removeShard: draining",
              "chunkCount"_attr = drainingProgress.totalChunks,
              "shardedChunkCount"_attr = drainingProgress.removeShardCounts.getChunks(),
              "unshardedCollectionsCount"_attr =
                  drainingProgress.removeShardCounts.getCollectionsToMove(),
              "databaseCount"_attr = drainingProgress.removeShardCounts.getDbs(),
              "jumboCount"_attr = drainingProgress.removeShardCounts.getJumboChunks());
        RemoveShardProgress progress(ShardDrainingStateEnum::kOngoing);
        progress.setRemaining(drainingProgress.removeShardCounts);
        return progress;
    }

    if (shardId == ShardId::kConfigServerId) {
        topology_change_helpers::joinMigrations(opCtx);
        // The config server may be added as a shard again, so we locally drop its drained
        // sharded collections to enable that without user intervention. But we have to wait for
        // the range deleter to quiesce to give queries and stale routers time to discover the
        // migration, to match the usual probabilistic guarantees for migrations.
        auto pendingRangeDeletions = topology_change_helpers::getRangeDeletionCount(opCtx);
        if (pendingRangeDeletions > 0) {
            LOGV2(7564600,
                  "removeShard: waiting for range deletions",
                  "pendingRangeDeletions"_attr = pendingRangeDeletions);
            RemoveShardProgress progress(ShardDrainingStateEnum::kPendingDataCleanup);
            progress.setPendingRangeDeletions(pendingRangeDeletions);
            return progress;
        }
    }

    // Prevent new ShardingDDLCoordinators operations from starting across the cluster.
    ScopeGuard unblockDDLCoordinatorsGuard([&] { scheduleAsyncUnblockDDLCoordinators(opCtx); });
    if (feature_flags::gStopDDLCoordinatorsDuringTopologyChanges.isEnabled(
            serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
        topology_change_helpers::blockDDLCoordinatorsAndDrain(opCtx);
    }

    hangRemoveShardAfterDrainingDDL.pauseWhileSet(opCtx);

    // Now that DDL operations are not executing, recheck that this shard truly does not own any
    // chunks nor database.
    drainingProgress =
        topology_change_helpers::getDrainingProgress(opCtx, _localConfigShard, shardName);
    // The counters: `shardedChunks`, `totalCollections`, and `databases` are used to present the
    // ongoing status to the user. Additionally, `totalChunks` on the shard is checked for safety,
    // as it is a critical point in the removeShard process, to ensure that a non-empty shard is not
    // removed. For example the number of unsharded collections might be inaccurate due to
    // $listClusterCatalog potentially returning an incorrect list of shards during concurrent DDL
    // operations.
    if (!drainingProgress.isFullyDrained()) {
        // Still more draining to do
        LOGV2(5687909,
              "removeShard: more draining to do after having blocked DDLCoordinators",
              "chunkCount"_attr = drainingProgress.totalChunks,
              "shardedChunkCount"_attr = drainingProgress.removeShardCounts.getChunks(),
              "unshardedCollectionsCount"_attr =
                  drainingProgress.removeShardCounts.getCollectionsToMove(),
              "databaseCount"_attr = drainingProgress.removeShardCounts.getDbs(),
              "jumboCount"_attr = drainingProgress.removeShardCounts.getJumboChunks());
        RemoveShardProgress progress(ShardDrainingStateEnum::kOngoing);
        progress.setRemaining(drainingProgress.removeShardCounts);
        return progress;
    }

    {
        // Keep the FCV stable across the commit of the shard removal. This allows us to only drop
        // the sessions collection when needed.
        //
        // NOTE: We don't use a Global IX lock here, because we don't want to hold the global lock
        // while blocking on the network).
        FixedFCVRegion fcvRegion(opCtx);

        if (shardId == ShardId::kConfigServerId) {
            auto trackedDBs =
                _localCatalogClient->getAllDBs(opCtx, repl::ReadConcernLevel::kLocalReadConcern);

            if (auto pendingCleanupState =
                    topology_change_helpers::dropLocalCollectionsAndDatabases(
                        opCtx, trackedDBs, shardName)) {
                return *pendingCleanupState;
            }

            // Also drop the sessions collection, which we assume is the only sharded collection in
            // the config database. Only do this if
            // featureFlagSessionsCollectionCoordinatorOnConfigServer is disabled. We don't have
            // synchronization with setFCV here, so it is still possible for rare interleavings to
            // drop the collection when they shouldn't, but the create coordinator will re-create it
            // on the next periodic refresh.
            if (!feature_flags::gSessionsCollectionCoordinatorOnConfigServer.isEnabled(
                    fcvRegion->acquireFCVSnapshot())) {
                DBDirectClient client(opCtx);
                BSONObj result;
                if (!client.dropCollection(NamespaceString::kLogicalSessionsNamespace,
                                           ShardingCatalogClient::kLocalWriteConcern,
                                           &result)) {
                    uassertStatusOK(getStatusFromCommandResult(result));
                }
            }
        }

        // Draining is done, now finish removing the shard.
        LOGV2(21949, "Going to remove shard", "shardId"_attr = shardName);

        // Synchronize the control shard selection, the shard's document removal, and the topology
        // time update to exclude potential race conditions in case of concurrent add/remove shard
        // operations.
        clusterCardinalityParameterLock.lock();
        shardMembershipLock.lock();

        topology_change_helpers::removeShard(
            shardMembershipLock,
            opCtx,
            _localConfigShard,
            shardName,
            Grid::get(opCtx)->getExecutorPool()->getFixedExecutor());

        // Release the shard membership lock since the set cluster parameter operation below
        // require taking this lock.
        shardMembershipLock.unlock();

        // Unset the addOrRemoveShardInProgress cluster parameter. Note that
        // _removeShardInTransaction has already waited for the commit to be majority-acknowledged.
        if (feature_flags::gStopDDLCoordinatorsDuringTopologyChanges.isEnabled(
                serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
            // TODO SERVER-99708: Investigate if we can remove this. unblockDDLCoordinators ends up
            // calling the configSvrSetClusterParameter command which takes the FCV lock. At this
            // point we are still holding the clusterCardinalityParameterLock. Other operations
            // proceed to take the locks in the inverse order, that is, they first take the FCV lock
            // followed by the clusterCardinalityParameterLock. We should review if we must unlock
            // the clusterCardinalityParameterLock here to prevent a lock cycle or see if this can
            // be refactored to prevent it.
            DisableLockerRuntimeOrderingChecks disableChecks{opCtx};
            topology_change_helpers::unblockDDLCoordinators(opCtx);
        }
        unblockDDLCoordinatorsGuard.dismiss();

        // The shard which was just removed must be reflected in the shard registry, before the
        // replica set monitor is removed, otherwise the shard would be referencing a dropped RSM.
        Grid::get(opCtx)->shardRegistry()->reload(opCtx);

        if (shardId != ShardId::kConfigServerId) {
            // Don't remove the config shard's RSM because it is used to target the config server.
            ReplicaSetMonitor::remove(replicaSetName);
        }

        // Record finish in changelog
        ShardingLogging::get(opCtx)->logChange(opCtx,
                                               "removeShard",
                                               NamespaceString::kEmpty,
                                               BSON("shard" << shardName),
                                               ShardingCatalogClient::kLocalWriteConcern,
                                               _localConfigShard,
                                               _localCatalogClient.get());

        hangRemoveShardBeforeUpdatingClusterCardinalityParameter.pauseWhileSet(opCtx);

        uassertStatusOK(_updateClusterCardinalityParameterAfterRemoveShardIfNeeded(
            clusterCardinalityParameterLock, opCtx));

        return RemoveShardProgress(ShardDrainingStateEnum::kCompleted);
    }
}

void ShardingCatalogManager::appendDBAndCollDrainingInfo(OperationContext* opCtx,
                                                         BSONObjBuilder& result,
                                                         ShardId shardId) {
    const auto databases =
        uassertStatusOK(_localCatalogClient->getDatabasesForShard(opCtx, shardId));

    const auto collections = getCollectionsToMoveForShard(opCtx, _localConfigShard.get(), shardId);

    // Get BSONObj containing:
    // 1) note about moving or dropping databases in a shard
    // 2) list of databases (excluding 'local' database) that need to be moved
    std::vector<DatabaseName> userDatabases;
    for (const auto& dbName : databases) {
        if (!dbName.isLocalDB()) {
            userDatabases.push_back(dbName);
        }
    }

    if (!userDatabases.empty() || !collections.empty()) {
        result.append("note",
                      "you need to call moveCollection for collectionsToMove and "
                      "afterwards movePrimary for the dbsToMove");
    }

    // Note the `dbsToMove` field could be excluded if we have no user database to move but we
    // enforce it for backcompatibilty.
    BSONArrayBuilder dbs(result.subarrayStart("dbsToMove"));
    bool canAppendToDoc = true;
    // The dbsToMove and collectionsToMove arrays will be truncated accordingly if they exceed
    // the 16MB BSON limitation. The offset for calculation is set to 10K to reserve place for
    // other attributes.
    int reservationOffsetBytes = 10 * 1024 + dbs.len();

    const auto maxUserSize = std::invoke([&] {
        if (auto failpoint = changeBSONObjMaxUserSize.scoped();
            MONGO_unlikely(failpoint.isActive())) {
            return failpoint.getData()["maxUserSize"].Int();
        } else {
            return BSONObjMaxUserSize;
        }
    });

    for (const auto& dbName : userDatabases) {
        canAppendToDoc = appendToArrayIfRoom(
            reservationOffsetBytes,
            dbs,
            DatabaseNameUtil::serialize(dbName, SerializationContext::stateDefault()),
            maxUserSize);
        if (!canAppendToDoc)
            break;
    }
    dbs.doneFast();
    reservationOffsetBytes += dbs.len();

    BSONArrayBuilder collectionsToMove(result.subarrayStart("collectionsToMove"));
    if (canAppendToDoc) {
        for (const auto& collectionName : collections) {
            canAppendToDoc =
                appendToArrayIfRoom(reservationOffsetBytes,
                                    collectionsToMove,
                                    NamespaceStringUtil::serialize(
                                        collectionName, SerializationContext::stateDefault()),
                                    maxUserSize);
            if (!canAppendToDoc)
                break;
        }
    }
    collectionsToMove.doneFast();
    if (!canAppendToDoc) {
        result.appendElements(BSON("truncated" << true));
    }
}

void ShardingCatalogManager::appendShardDrainingStatus(OperationContext* opCtx,
                                                       BSONObjBuilder& result,
                                                       RemoveShardProgress shardDrainingStatus,
                                                       ShardId shardId) {
    const auto& state = shardDrainingStatus.getState();
    if (state == ShardDrainingStateEnum::kStarted || state == ShardDrainingStateEnum::kOngoing) {
        appendDBAndCollDrainingInfo(opCtx, result, shardId);
    }
    if (state == ShardDrainingStateEnum::kStarted || state == ShardDrainingStateEnum::kCompleted) {
        result.append("shard", shardId);
    }
    result.append("msg", topology_change_helpers::getRemoveShardMessage(state));
    shardDrainingStatus.serialize(&result);
}

Lock::SharedLock ShardingCatalogManager::enterStableTopologyRegion(OperationContext* opCtx) {
    return Lock::SharedLock(opCtx, _kShardMembershipLock);
}

Lock::ExclusiveLock ShardingCatalogManager::acquireShardMembershipLockForTopologyChange(
    OperationContext* opCtx) {
    return Lock::ExclusiveLock(opCtx, _kShardMembershipLock);
}

void ShardingCatalogManager::appendConnectionStats(executor::ConnectionPoolStats* stats) {
    _executorForAddShard->appendConnectionStats(stats);
}

void ShardingCatalogManager::_setUserWriteBlockingStateOnNewShard(OperationContext* opCtx,
                                                                  RemoteCommandTargeter* targeter) {

    uint8_t level = topology_change_helpers::UserWriteBlockingLevel::None;

    // Propagate the cluster's current user write blocking state onto the new shard.
    PersistentTaskStore<UserWriteBlockingCriticalSectionDocument> store(
        NamespaceString::kUserWritesCriticalSectionsNamespace);
    store.forEach(opCtx, BSONObj(), [&](const UserWriteBlockingCriticalSectionDocument& doc) {
        invariant(doc.getNss() ==
                  UserWritesRecoverableCriticalSectionService::kGlobalUserWritesNamespace);

        if (doc.getBlockNewUserShardedDDL()) {
            level |= topology_change_helpers::UserWriteBlockingLevel::DDLOperations;
        }

        if (doc.getBlockUserWrites()) {
            invariant(doc.getBlockNewUserShardedDDL());
            level |= topology_change_helpers::UserWriteBlockingLevel::Writes;
        }

        return true;
    });

    topology_change_helpers::setUserWriteBlockingState(
        opCtx,
        *targeter,
        topology_change_helpers::UserWriteBlockingLevel(level),
        true,
        boost::none,
        _executorForAddShard);
}

Status ShardingCatalogManager::_pullClusterTimeKeys(
    OperationContext* opCtx, std::shared_ptr<RemoteCommandTargeter> targeter) {
    Status fetchStatus =
        Status(ErrorCodes::InternalError, "Internal error running cursor callback in command");
    std::vector<ExternalKeysCollectionDocument> keyDocs;

    auto expireAt = opCtx->getServiceContext()->getFastClockSource()->now() +
        Seconds(gNewShardExistingClusterTimeKeysExpirationSecs.load());
    auto fetcher = topology_change_helpers::createFetcher(
        opCtx,
        *targeter,
        NamespaceString::kKeysCollectionNamespace,
        repl::ReadConcernLevel::kLocalReadConcern,
        [&](const std::vector<BSONObj>& docs) -> bool {
            for (const BSONObj& doc : docs) {
                keyDocs.push_back(
                    keys_collection_util::makeExternalClusterTimeKeyDoc(doc.getOwned(), expireAt));
            }
            return true;
        },
        [&](const Status& status) { fetchStatus = status; },
        _executorForAddShard);

    auto scheduleStatus = fetcher->schedule();
    if (!scheduleStatus.isOK()) {
        return scheduleStatus;
    }

    auto joinStatus = fetcher->join(opCtx);
    if (!joinStatus.isOK()) {
        return joinStatus;
    }

    if (keyDocs.empty()) {
        return fetchStatus;
    }

    auto opTime = keys_collection_util::storeExternalClusterTimeKeyDocs(opCtx, std::move(keyDocs));
    auto waitStatus = WaitForMajorityService::get(opCtx->getServiceContext())
                          .waitUntilMajorityForWrite(opTime, opCtx->getCancellationToken())
                          .getNoThrow();
    if (!waitStatus.isOK()) {
        return waitStatus;
    }

    return fetchStatus;
}

void ShardingCatalogManager::_setClusterParametersLocally(OperationContext* opCtx,
                                                          const std::vector<BSONObj>& parameters) {
    DBDirectClient client(opCtx);
    ClusterParameterDBClientService dbService(client);
    const auto tenantId = [&]() -> boost::optional<TenantId> {
        const auto vts = auth::ValidatedTenancyScope::get(opCtx);
        invariant(!vts || vts->hasTenantId());

        if (vts && vts->hasTenantId()) {
            return vts->tenantId();
        }
        return boost::none;
    }();

    for (auto& parameter : parameters) {
        SetClusterParameter setClusterParameterRequest(
            BSON(parameter["_id"].String() << parameter.filterFieldsUndotted(
                     BSON("_id" << 1 << "clusterParameterTime" << 1), false)));
        setClusterParameterRequest.setDbName(DatabaseNameUtil::deserialize(
            tenantId, DatabaseName::kAdmin.db(omitTenant), SerializationContext::stateDefault()));
        std::unique_ptr<ServerParameterService> parameterService =
            std::make_unique<ClusterParameterService>();
        SetClusterParameterInvocation invocation{std::move(parameterService), dbService};
        invocation.invoke(opCtx,
                          setClusterParameterRequest,
                          parameter["clusterParameterTime"].timestamp(),
                          boost::none /* previousTime */,
                          kMajorityWriteConcern);
    }
}

void ShardingCatalogManager::_pullClusterParametersFromNewShard(OperationContext* opCtx,
                                                                Shard* shard) {
    const auto& targeter = shard->getTargeter();
    LOGV2(6538600, "Pulling cluster parameters from new shard");

    // We can safely query the cluster parameters because the replica set must have been started
    // with --shardsvr in order to add it into the cluster, and in this mode no setClusterParameter
    // can be called on the replica set directly.
    auto tenantIds =
        uassertStatusOK(getTenantsWithConfigDbsOnShard(opCtx, shard, _executorForAddShard.get()));

    std::vector<std::unique_ptr<Fetcher>> fetchers;
    fetchers.reserve(tenantIds.size());
    // If for some reason the callback never gets invoked, we will return this status in
    // response.
    std::vector<Status> statuses(
        tenantIds.size(),
        Status(ErrorCodes::InternalError, "Internal error running cursor callback in command"));
    std::vector<std::vector<BSONObj>> allParameters(tenantIds.size());

    int i = 0;
    for (const auto& tenantId : tenantIds) {
        auto fetcher = topology_change_helpers::createFetcher(
            opCtx,
            *targeter,
            NamespaceString::makeClusterParametersNSS(tenantId),
            repl::ReadConcernLevel::kMajorityReadConcern,
            [&allParameters, i](const std::vector<BSONObj>& docs) -> bool {
                std::vector<BSONObj> parameters;
                parameters.reserve(docs.size());
                for (const BSONObj& doc : docs) {
                    parameters.push_back(doc.getOwned());
                }
                allParameters[i] = parameters;
                return true;
            },
            [&statuses, i](const Status& status) { statuses[i] = status; },
            _executorForAddShard);
        uassertStatusOK(fetcher->schedule());
        fetchers.push_back(std::move(fetcher));
        i++;
    }

    i = 0;
    for (const auto& tenantId : tenantIds) {
        uassertStatusOK(fetchers[i]->join(opCtx));
        uassertStatusOK(statuses[i]);

        auth::ValidatedTenancyScopeGuard::runAsTenant(opCtx, tenantId, [&]() -> void {
            _setClusterParametersLocally(opCtx, allParameters[i]);
        });
        i++;
    }
}

void ShardingCatalogManager::_removeAllClusterParametersFromShard(OperationContext* opCtx,
                                                                  Shard* shard) {
    const auto& targeter = shard->getTargeter();
    auto tenantsOnTarget =
        uassertStatusOK(getTenantsWithConfigDbsOnShard(opCtx, shard, _executorForAddShard.get()));

    // Remove possible leftovers config.clusterParameters documents from the new shard.
    for (const auto& tenantId : tenantsOnTarget) {
        const auto& nss = NamespaceString::makeClusterParametersNSS(tenantId);
        write_ops::DeleteCommandRequest deleteOp(nss);
        write_ops::DeleteOpEntry query({}, true /*multi*/);
        deleteOp.setDeletes({query});
        generic_argument_util::setMajorityWriteConcern(deleteOp);

        const auto swCommandResponse =
            _runCommandForAddShard(opCtx, targeter.get(), nss.dbName(), deleteOp.toBSON());
        uassertStatusOK(swCommandResponse.getStatus());
        uassertStatusOK(getStatusFromWriteCommandReply(swCommandResponse.getValue().response));
    }
}

void ShardingCatalogManager::_pushClusterParametersToNewShard(
    OperationContext* opCtx,
    Shard* shard,
    const TenantIdMap<std::vector<BSONObj>>& allClusterParameters) {
    // First, remove all existing parameters from the new shard.
    _removeAllClusterParametersFromShard(opCtx, shard);

    const auto& targeter = shard->getTargeter();
    LOGV2(6360600, "Pushing cluster parameters into new shard");

    for (const auto& [tenantId, clusterParameters] : allClusterParameters) {
        const auto& dbName = DatabaseNameUtil::deserialize(
            tenantId, DatabaseName::kAdmin.db(omitTenant), SerializationContext::stateDefault());
        // Push cluster parameters into the newly added shard.
        for (auto& parameter : clusterParameters) {
            ShardsvrSetClusterParameter setClusterParamsCmd(
                BSON(parameter["_id"].String() << parameter.filterFieldsUndotted(
                         BSON("_id" << 1 << "clusterParameterTime" << 1), false)));
            setClusterParamsCmd.setDbName(dbName);
            setClusterParamsCmd.setClusterParameterTime(
                parameter["clusterParameterTime"].timestamp());
            generic_argument_util::setMajorityWriteConcern(setClusterParamsCmd);

            const auto cmdResponse =
                _runCommandForAddShard(opCtx, targeter.get(), dbName, setClusterParamsCmd.toBSON());
            uassertStatusOK(Shard::CommandResponse::getEffectiveStatus(cmdResponse));
        }
    }
}

void ShardingCatalogManager::_standardizeClusterParameters(OperationContext* opCtx, Shard* shard) {
    auto tenantIds =
        uassertStatusOK(getTenantsWithConfigDbsOnShard(opCtx, _localConfigShard.get()));
    TenantIdMap<std::vector<BSONObj>> configSvrClusterParameterDocs;
    for (const auto& tenantId : tenantIds) {
        auto findResponse = uassertStatusOK(_localConfigShard->exhaustiveFindOnConfig(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString::makeClusterParametersNSS(tenantId),
            BSONObj(),
            BSONObj(),
            boost::none));

        configSvrClusterParameterDocs.emplace(tenantId, findResponse.docs);
    }

    auto shardsDocs = uassertStatusOK(_localConfigShard->exhaustiveFindOnConfig(
        opCtx,
        ReadPreferenceSetting(ReadPreference::PrimaryOnly),
        repl::ReadConcernLevel::kLocalReadConcern,
        NamespaceString::kConfigsvrShardsNamespace,
        BSONObj(),
        BSONObj(),
        boost::none));

    // If this is the first shard being added, and no cluster parameters have been set, then this
    // can be seen as a replica set to shard conversion -- absorb all of this shard's cluster
    // parameters. Otherwise, push our cluster parameters to the shard.
    if (shardsDocs.docs.empty()) {
        bool clusterParameterDocsEmpty = std::all_of(
            configSvrClusterParameterDocs.begin(),
            configSvrClusterParameterDocs.end(),
            [&](const std::pair<boost::optional<TenantId>, std::vector<BSONObj>>& tenantParams) {
                return tenantParams.second.empty();
            });
        if (clusterParameterDocsEmpty) {
            _pullClusterParametersFromNewShard(opCtx, shard);
            return;
        }
    }
    _pushClusterParametersToNewShard(opCtx, shard, configSvrClusterParameterDocs);
}

void ShardingCatalogManager::_addShardInTransaction(
    OperationContext* opCtx,
    const ShardType& newShard,
    std::vector<DatabaseName>&& databasesInNewShard,
    std::vector<CollectionType>&& collectionsInNewShard) {

    const auto existingShardIds = Grid::get(opCtx)->shardRegistry()->getAllShardIds(opCtx);

    const auto collCreationTime = [&]() {
        const auto currentTime = VectorClock::get(opCtx)->getTime();
        return currentTime.clusterTime().asTimestamp();
    }();
    for (auto& coll : collectionsInNewShard) {
        coll.setTimestamp(collCreationTime);
    }

    // Set up and run the commit statements
    // TODO SERVER-81582: generate batches of transactions to insert the database/placementHistory
    // and collection/placementHistory before adding the shard in config.shards.
    auto transactionChain = [opCtx, &newShard, &databasesInNewShard, &collectionsInNewShard](
                                const txn_api::TransactionClient& txnClient, ExecutorPtr txnExec) {
        write_ops::InsertCommandRequest insertShardEntry(NamespaceString::kConfigsvrShardsNamespace,
                                                         {newShard.toBSON()});
        return txnClient.runCRUDOp(insertShardEntry, {})
            .thenRunOn(txnExec)
            .then([&](const BatchedCommandResponse& insertShardEntryResponse) {
                uassertStatusOK(insertShardEntryResponse.toStatus());
                if (databasesInNewShard.empty()) {
                    BatchedCommandResponse noOpResponse;
                    noOpResponse.setStatus(Status::OK());
                    noOpResponse.setN(0);

                    return SemiFuture<BatchedCommandResponse>(std::move(noOpResponse));
                }

                std::vector<BSONObj> databaseEntries;
                std::transform(databasesInNewShard.begin(),
                               databasesInNewShard.end(),
                               std::back_inserter(databaseEntries),
                               [&](const DatabaseName& dbName) {
                                   return DatabaseType(dbName,
                                                       newShard.getName(),
                                                       DatabaseVersion(UUID::gen(),
                                                                       newShard.getTopologyTime()))
                                       .toBSON();
                               });
                write_ops::InsertCommandRequest insertDatabaseEntries(
                    NamespaceString::kConfigDatabasesNamespace, std::move(databaseEntries));
                return txnClient.runCRUDOp(insertDatabaseEntries, {});
            })
            .thenRunOn(txnExec)
            .then([&](const BatchedCommandResponse& insertDatabaseEntriesResponse) {
                uassertStatusOK(insertDatabaseEntriesResponse.toStatus());
                if (collectionsInNewShard.empty()) {
                    BatchedCommandResponse noOpResponse;
                    noOpResponse.setStatus(Status::OK());
                    noOpResponse.setN(0);

                    return SemiFuture<BatchedCommandResponse>(std::move(noOpResponse));
                }
                std::vector<BSONObj> collEntries;

                std::transform(collectionsInNewShard.begin(),
                               collectionsInNewShard.end(),
                               std::back_inserter(collEntries),
                               [&](const CollectionType& coll) { return coll.toBSON(); });
                write_ops::InsertCommandRequest insertCollectionEntries(
                    NamespaceString::kConfigsvrCollectionsNamespace, std::move(collEntries));
                return txnClient.runCRUDOp(insertCollectionEntries, {});
            })
            .thenRunOn(txnExec)
            .then([&](const BatchedCommandResponse& insertCollectionEntriesResponse) {
                uassertStatusOK(insertCollectionEntriesResponse.toStatus());
                if (collectionsInNewShard.empty()) {
                    BatchedCommandResponse noOpResponse;
                    noOpResponse.setStatus(Status::OK());
                    noOpResponse.setN(0);

                    return SemiFuture<BatchedCommandResponse>(std::move(noOpResponse));
                }
                std::vector<BSONObj> chunkEntries;
                const auto unsplittableShardKey =
                    ShardKeyPattern(sharding_ddl_util::unsplittableCollectionShardKey());
                const auto shardId = ShardId(newShard.getName());
                std::transform(
                    collectionsInNewShard.begin(),
                    collectionsInNewShard.end(),
                    std::back_inserter(chunkEntries),
                    [&](const CollectionType& coll) {
                        // Create a single chunk for this
                        ChunkType chunk(
                            coll.getUuid(),
                            {coll.getKeyPattern().globalMin(), coll.getKeyPattern().globalMax()},
                            {{coll.getEpoch(), coll.getTimestamp()}, {1, 0}},
                            shardId);
                        chunk.setOnCurrentShardSince(coll.getTimestamp());
                        chunk.setHistory({ChunkHistory(*chunk.getOnCurrentShardSince(), shardId)});
                        return chunk.toConfigBSON();
                    });

                write_ops::InsertCommandRequest insertChunkEntries(
                    NamespaceString::kConfigsvrChunksNamespace, std::move(chunkEntries));
                return txnClient.runCRUDOp(insertChunkEntries, {});
            })
            .thenRunOn(txnExec)
            .then([&](const BatchedCommandResponse& insertChunkEntriesResponse) {
                uassertStatusOK(insertChunkEntriesResponse.toStatus());
                if (databasesInNewShard.empty()) {
                    BatchedCommandResponse noOpResponse;
                    noOpResponse.setStatus(Status::OK());
                    noOpResponse.setN(0);

                    return SemiFuture<BatchedCommandResponse>(std::move(noOpResponse));
                }
                std::vector<BSONObj> placementEntries;
                std::transform(databasesInNewShard.begin(),
                               databasesInNewShard.end(),
                               std::back_inserter(placementEntries),
                               [&](const DatabaseName& dbName) {
                                   return NamespacePlacementType(NamespaceString(dbName),
                                                                 newShard.getTopologyTime(),
                                                                 {ShardId(newShard.getName())})
                                       .toBSON();
                               });
                std::transform(collectionsInNewShard.begin(),
                               collectionsInNewShard.end(),
                               std::back_inserter(placementEntries),
                               [&](const CollectionType& coll) {
                                   NamespacePlacementType placementInfo(
                                       NamespaceString(coll.getNss()),
                                       coll.getTimestamp(),
                                       {ShardId(newShard.getName())});
                                   placementInfo.setUuid(coll.getUuid());

                                   return placementInfo.toBSON();
                               });
                write_ops::InsertCommandRequest insertPlacementEntries(
                    NamespaceString::kConfigsvrPlacementHistoryNamespace,
                    std::move(placementEntries));
                return txnClient.runCRUDOp(insertPlacementEntries, {});
            })
            .thenRunOn(txnExec)
            .then([](auto insertPlacementEntriesResponse) {
                uassertStatusOK(insertPlacementEntriesResponse.toStatus());
            })
            .semi();
    };

    {
        auto& executor = Grid::get(opCtx)->getExecutorPool()->getFixedExecutor();
        auto inlineExecutor = std::make_shared<executor::InlineExecutor>();

        txn_api::SyncTransactionWithRetries txn(opCtx, executor, nullptr, inlineExecutor);
        txn.run(opCtx, transactionChain);
    }
}

void ShardingCatalogManager::scheduleAsyncUnblockDDLCoordinators(OperationContext* opCtx) {
    if (!mongo::feature_flags::gStopDDLCoordinatorsDuringTopologyChanges.isEnabled(
            serverGlobalParams.featureCompatibility.acquireFCVSnapshot())) {
        return;
    }

    auto executor = Grid::get(opCtx)->getExecutorPool()->getFixedExecutor();
    const auto serviceContext = opCtx->getServiceContext();
    AsyncTry([this, serviceContext] {
        ThreadClient tc{"resetDDLBlockingForTopologyChange",
                        serviceContext->getService(ClusterRole::ShardServer)};
        auto uniqueOpCtx{tc->makeOperationContext()};
        auto opCtx{uniqueOpCtx.get()};

        // Take _kAddRemoveShardLock to ensure that the reset does not interleave with a new
        // addShard/removeShard command.
        Lock::ExclusiveLock addRemoveShardLock(opCtx, _kAddRemoveShardLock);
        resetDDLBlockingForTopologyChangeIfNeeded(opCtx);
    })
        .until([serviceContext](Status status) {
            // Retry until success or until this node is no longer the primary.
            const bool primary =
                repl::ReplicationCoordinator::get(serviceContext)->getMemberState().primary();
            return status.isOK() || !primary;
        })
        .withDelayBetweenIterations(Seconds(1))
        .on(executor, CancellationToken::uncancelable())
        .onError([](Status status) {
            LOGV2_WARNING(
                5687908,
                "Failed to reset addOrRemoveShardInProgress cluster parameter after failure",
                "error"_attr = status.toString());
        })
        .getAsync([](auto) {});
}

}  // namespace mongo
