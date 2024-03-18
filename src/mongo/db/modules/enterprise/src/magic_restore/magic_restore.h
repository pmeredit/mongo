/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
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
 * Performs follow-up steps for sharded clusters.
 */
void updateShardingMetadata(OperationContext* opCtx,
                            const RestoreConfiguration& restoreConfig,
                            repl::StorageInterface* storageInterface);
/**
 * Reads oplog entries from the BSONStreamReader and inserts them into the oplog. Each entry is
 * inserted in its own write unit of work. Note that the function will hold on to the global lock in
 * IX mode for the duration of oplog entry insertion.
 */
void writeOplogEntriesToOplog(ServiceContext* svcCtx, const BSONStreamReader& reader);

ExitCode magicRestoreMain(ServiceContext* svcCtx);

}  // namespace magic_restore
}  // namespace mongo
