/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/operation_context.h"
#include "mongo/util/future.h"
#include "mongo/util/uuid.h"

namespace mongo {

/**
 * Coordinate a dry run of importing a collection in a replica set.
 */
class ImportCollectionCoordinator {
public:
    /**
     * Provide access to the ImportCollectionCoordinator decoration on the ServiceContext.
     */
    static ImportCollectionCoordinator* get(ServiceContext* serviceContext);
    static ImportCollectionCoordinator* get(OperationContext* operationContext);

    ImportCollectionCoordinator() = default;
    ~ImportCollectionCoordinator() = default;

    SharedSemiFuture<void> registerImport(const UUID& importUUID);
    void unregisterImport(const UUID& importUUID);
    void voteForImport(const UUID& importUUID,
                       const HostAndPort& host,
                       bool success,
                       const boost::optional<StringData>& reasons = boost::none);

private:
    stdx::mutex _mutex;

    struct DryRunStatus {
        SharedPromise<void> promise;
        stdx::unordered_set<HostAndPort> readyMembers;
    };
    stdx::unordered_map<UUID, DryRunStatus, UUID::Hash> _currentDryRuns;
};

}  // namespace mongo
