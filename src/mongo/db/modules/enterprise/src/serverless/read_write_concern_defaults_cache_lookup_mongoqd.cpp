/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"

#include "read_write_concern_defaults_cache_lookup_mongoqd.h"

#include "mongo/db/commands/rwc_defaults_commands_gen.h"
#include "mongo/s/grid.h"

namespace mongo {

boost::optional<RWConcernDefault> readWriteConcernDefaultsCacheLookupMongoQD(
    OperationContext* opCtx) {
    GetDefaultRWConcern configsvrRequest;
    configsvrRequest.setDbName(DatabaseName::kAdmin);

    auto configShard = Grid::get(opCtx)->shardRegistry()->getConfigShard();
    auto cmdResponse = uassertStatusOK(configShard->runCommandWithFixedRetryAttempts(
        opCtx,
        ReadPreferenceSetting(ReadPreference::Nearest),
        DatabaseName::kAdmin,
        configsvrRequest.toBSON(),
        Shard::RetryPolicy::kIdempotent));

    uassertStatusOK(cmdResponse.commandStatus);

    return RWConcernDefault::parse(IDLParserContext("readWriteConcernDefaultsCacheLookupMongoQD"),
                                   cmdResponse.response);
}

}  // namespace mongo
