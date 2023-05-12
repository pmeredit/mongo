/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_commands_gen.h"

namespace mongo {

/**
 * Runs the given command against the remote search index management server, if the remote host
 * information has been set via 'searchIndexManagementHostAndPort'.
 */
BSONObj runSearchIndexCommand(OperationContext* opCtx,
                              const NamespaceString& nss,
                              const BSONObj& cmdObj);

}  // namespace mongo
