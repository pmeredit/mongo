/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/read_write_concern_defaults.h"

namespace mongo {

/**
 * A function which handles looking up RWConcernDefault values from config servers.
 */
boost::optional<RWConcernDefault> readWriteConcernDefaultsCacheLookupMongoQD(
    OperationContext* opCtx);

}  // namespace mongo
