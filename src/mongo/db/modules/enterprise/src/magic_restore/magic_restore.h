/**
 * Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/service_context.h"
#include "mongo/util/exit_code.h"

namespace mongo {
namespace magic_restore {

/*
Implementation of a BSON reader that produces BSON objects from an arbitrary stream of data. The
reader expects well-formed input in the form of raw BSON data, and can be used with stdin or an
input stream opened on a file.
*/
class BSONStreamReader {
public:
    BSONStreamReader(std::istream& stream);

    BSONStreamReader() = delete;
    BSONStreamReader(const BSONStreamReader&) = delete;
    BSONStreamReader& operator=(const BSONStreamReader&) = delete;
    ~BSONStreamReader() = default;

    /**
     * Returns true if there is additional data in the stream.
     */
    bool hasNext();

    /**
     * Reads and returns the next BSON object from the stream.
     */
    BSONObj getNext();

    /**
     * Returns the total number of bytes read by the BSONStreamReader. This value includes the bytes
     * for the BSON sizes.
     */
    int64_t getTotalBytesRead();

    /**
     * Returns the total number of BSON objects read.
     */
    int64_t getTotalObjectsRead();

private:
    std::istream& _stream;
    // Stores one object from the stream at a time.
    std::unique_ptr<char[]> _buffer;

    const int64_t _bsonLengthHeaderSizeBytes = 4;
    int64_t _totalBytesRead = 0;
    int64_t _totalObjectsRead = 0;
};

const std::array<std::string, 3> approvedClusterParameters = {
    "defaultMaxTimeMS", "querySettings", "shardedClusterCardinalityForDirectConns"};

/**
 * Validates the magic restore configuration fields.
 */
class RestoreConfiguration;
void validateRestoreConfiguration(const RestoreConfiguration* config);

/**
 * Truncates the following collections in the local db:
 * - system.replset
 * - replset.oplogTruncateAfterPoint, replset.minvalid, replset.election, replset.initialSyncId
 */
void truncateLocalDbCollections(OperationContext* opCtx, repl::StorageInterface* storageInterface);

/**
 * Sets the singleton document in replset.minvalid with an invalid document.
 */
void setInvalidMinValid(OperationContext* opCtx, repl::StorageInterface* storageInterface);

/**
 * Updates the sharding metadata collections by replacing references from the source shard name to
 * the destination shard name as specified in the RestoreConfiguration. If no shard rename is
 * specified, the function returns early.
 */
void updateShardNameMetadata(OperationContext* opCtx,
                             const RestoreConfiguration& restoreConfig,
                             repl::StorageInterface* storageInterface);

/**
 * Create local.system.collections_to_restore, which is a metadata collection used to support a
 * selective restore that filters on namespaces. The restore procedure must remove config metadata
 * for unrestored collections.
 */
class NamespaceUUIDPair;
void createCollectionsToRestore(
    OperationContext* opCtx,
    const std::vector<mongo::magic_restore::NamespaceUUIDPair>& nsAndUuids,
    repl::StorageInterface* storageInterface);

/**
 * Helper function that runs the functions needed for a selective restore. This function will only
 * be called on a config server node. It creates the 'collections_to_restore' collection, populates
 * it with the given restored collections passed in via the restoreConfig, and runs the
 * _configsvrRunRestore command. This command removes metadata about unrestored collections. This
 * function then drops the 'collections_to_restore' collection before returning.
 */
void runSelectiveRestoreSteps(OperationContext* opCtx,
                              const RestoreConfiguration& restoreConfig,
                              repl::StorageInterface* storageInterface);


/**
 * Performs follow-up steps for sharded clusters.
 */
void updateShardingMetadata(OperationContext* opCtx,
                            const RestoreConfiguration& restoreConfig,
                            repl::StorageInterface* storageInterface);

/**
 * Drops all cluster parameters that are not specifically whitelisted for restore.
 */
void dropNonRestoredClusterParameters(OperationContext* opCtx,
                                      repl::StorageInterface* storageInterface);

/**
 * Reads oplog entries from the BSONStreamReader and inserts them into the oplog. Each entry is
 * inserted in its own write unit of work. Note that the function will hold on to the global lock in
 * IX mode for the duration of oplog entry insertion.
 */
void writeOplogEntriesToOplog(ServiceContext* svcCtx, const BSONStreamReader& reader);


/**
 * Helper function to check if the collection with the given namespace exists. If it doesn't, the
 * function will fatally assert.
 */
void checkInternalCollectionExists(OperationContext* opCtx, const NamespaceString& nss);

/**
 * Creates collections on the node with the given namespace and UUIDs. Ensures the collections have
 * the the same UUID across restored nodes in the same replica set.
 *
 */
void createInternalCollectionsWithUuid(
    OperationContext* opCtx,
    repl::StorageInterface* storageInterface,
    const std::vector<mongo::magic_restore::NamespaceUUIDPair>& nsAndUuids);

/**
 * Helper function to execute the automation agent credentials upsert. If a create command fails
 * with a DuplicateKey error, the function will convert the command into an update command run it
 * again.
 *
 */
void executeCredentialsCommand(OperationContext* opCtx,
                               const BSONObj& cmd,
                               repl::StorageInterface* storageInterface);

/**
 * Inserts automation credentials into admin.system.roles and admin.system.users. Attempts to run
 * createRole/createUser commands, but if the role/user already exists on the snapshotted data
 * files, converts the commands to updateRole/updateUser commands.
 */
class AutomationCredentials;
void upsertAutomationCredentials(OperationContext* opCtx,
                                 const AutomationCredentials& autoCreds,
                                 repl::StorageInterface* storageInterface);

/*
 * Inserts a no-op oplog entry with a term value influenced by the 'restoreToHigherTermThan' field
 * in the restore configuration. If the last oplog entry's term is higher than the
 * 'restoreToHigherTermThan' field, we'll keep that term value. The function will also retrieve the
 * 'ts' and 'wall' timestamps from the latest oplog entry in the oplog, increment them, and use them
 * in the no-op entry. Drivers maintain the last term it received from a replica set, and this field
 * is used to preserve those connections for an active restore. Returns the timestamp of the no-op
 * oplog entry.
 */
Timestamp insertHigherTermNoOpOplogEntry(OperationContext* opCtx,
                                         repl::StorageInterface* storageInterface,
                                         BSONObj& lastOplogEntry,
                                         long long higherTerm);

ExitCode magicRestoreMain(ServiceContext* svcCtx);

}  // namespace magic_restore
}  // namespace mongo
