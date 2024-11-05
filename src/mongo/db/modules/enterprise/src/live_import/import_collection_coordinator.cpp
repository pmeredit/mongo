/**
 * Copyright (C) 2020-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "import_collection_coordinator.h"

#include "mongo/db/index_builds/commit_quorum_options.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplication


namespace mongo {
namespace {
const auto _coordinatorDecoration =
    ServiceContext::declareDecoration<ImportCollectionCoordinator>();
}  // namespace

ImportCollectionCoordinator* ImportCollectionCoordinator::get(ServiceContext* serviceContext) {
    return &_coordinatorDecoration(serviceContext);
}

ImportCollectionCoordinator* ImportCollectionCoordinator::get(OperationContext* operationContext) {
    return get(operationContext->getServiceContext());
}

SharedSemiFuture<void> ImportCollectionCoordinator::registerImport(const UUID& importUUID) {
    stdx::lock_guard lk(_mutex);
    auto [iter, isNew] = _currentDryRuns.try_emplace(importUUID);
    // We should register an import only once.
    invariant(isNew);
    return iter->second.promise.getFuture();
}

void ImportCollectionCoordinator::unregisterImport(const UUID& importUUID) {
    stdx::lock_guard lk(_mutex);
    auto iter = _currentDryRuns.find(importUUID);
    // If the dryRun's promise has been fulfilled, it was already removed from the map.
    if (iter != _currentDryRuns.end()) {
        _currentDryRuns.erase(iter);
    }
}

void ImportCollectionCoordinator::voteForImport(const UUID& importUUID,
                                                const HostAndPort& host,
                                                bool success,
                                                const boost::optional<StringData>& reasons) {
    stdx::lock_guard lk(_mutex);
    auto iter = _currentDryRuns.find(importUUID);

    // Nothing to do if we don't find the import.
    if (iter == _currentDryRuns.end()) {
        return;
    }

    // Notify the import dryRun with an error.
    if (!success) {
        iter->second.promise.setError(
            {ErrorCodes::OperationFailed,
             str::stream() << "import dryRun failed on host " << host << ", error: " << reasons});
        // Remove from the map once the promise is fulfilled to avoid needing to worry about
        // fulfilling the promise multiple times.
        _currentDryRuns.erase(iter);
        return;
    }

    // Add the host to the readyMembers set.
    iter->second.readyMembers.insert(host);

    std::vector<HostAndPort> readyMembers(iter->second.readyMembers.begin(),
                                          iter->second.readyMembers.end());
    CommitQuorumOptions allVotingMembers(CommitQuorumOptions::kVotingMembers);
    auto ready = repl::ReplicationCoordinator::get(getGlobalServiceContext())
                     ->isCommitQuorumSatisfied(allVotingMembers, readyMembers);
    // Notify the import dryRun with an OK status.
    if (ready) {
        iter->second.promise.emplaceValue();
        // Remove from the map once the promise is fulfilled to avoid needing to worry about
        // fulfilling the promise multiple times.
        _currentDryRuns.erase(iter);
    }
}

}  // namespace mongo
