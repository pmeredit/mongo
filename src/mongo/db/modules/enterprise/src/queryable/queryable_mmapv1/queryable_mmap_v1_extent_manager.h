/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "mongo/db/storage/mmap_v1/extent.h"
#include "mongo/db/storage/mmap_v1/mmap_v1_extent_manager.h"
#include "mongo/db/storage/mmap_v1/record.h"
#include "mongo/db/storage/record_fetcher.h"

#include "queryable_alloc_state.h"
#include "queryable_datafile.h"

#include "../blockstore/context.h"

namespace mongo {

class DiskLoc;
struct Extent;
class OperationContext;

namespace queryable {

/**
 * Note this algorithm is still young, so don't take these words as gospel. It's nevertheless a good
 * idea to give readers some* idea of what's going on.
 *
 * MMAPv1 requires all `.ns` to be loaded before starting, thus the first task of the
 * BlockstoreBackedExtentManager is to download all `.ns` files to the `dbpath` and mmap them.
 *
 * Otherwise, MMAPv1 guarantees to call either `recordForV1` or `getExtent` (both take a `DiskLoc`)
 * before accessing[1] data at that `DiskLoc`. Broadly speaking (see method documentation for
 * details), this implementation uses those methods to translate the `DiskLoc` to a block(s) to
 * download, writes the value of the block to memory at the correct offset and returns a pointer to
 * the object the caller requested.
 *
 * [1] "before accessing" is correct, but not necessarily precise. An adverserial case can have a
 * cursor call `recordForV1`, but wait arbitrarily long before accessing the data. This can make
 * paging data out to satisfy memory constraints difficult.
 */
class BlockstoreBackedExtentManager final : public MmapV1ExtentManager {
public:
    class Factory final : public ExtentManager::Factory {
    public:
        Factory(Context&& context, std::uint64_t memoryQuotaBytes);

        Context* getContext() {
            return &_context;
        }

        std::unique_ptr<ExtentManager> create(StringData dbname,
                                              StringData path,
                                              bool directoryPerDB) override;

    private:
        Context _context;
        // `_allocState` shared across the entire queryable_mmapv1 system.
        std::unique_ptr<AllocState> _allocState;
    };

    BlockstoreBackedExtentManager(Factory* factory,
                                  StringData dbname,
                                  StringData path,
                                  bool directoryPerDB,
                                  AllocState* allocState);

    Status init(OperationContext* opCtx) override;

    /**
     * The input `DiskLoc` can be a location to a btree node or a Collection's BSON document. Both
     * cases encode their size in the header, so this method will first download the
     * `MmapV1RecordHeader` (first 16 bytes from the pointer). From those bytes, it can determine
     * the size of the record's contents and download any remaining data.
     */
    MmapV1RecordHeader* recordForV1(const DiskLoc& loc) const override;

    /**
     * Similar to `recordForV1`, but only needs to download the `Extent::HeaderSize`. Calls made
     * within the extent go through `recordForV1`.
     */
    Extent* getExtent(const DiskLoc& loc, bool) const override;

    /**
     * The default implementation is problematic. Perform a no-op.
     */
    std::unique_ptr<RecordFetcher> recordNeedsFetch(const DiskLoc& loc) const override {
        return {};
    }

private:
    Factory* const _factory;
    const std::string _dbname;
    const bool _directoryPerDB;
    std::vector<std::unique_ptr<DataFile>> _dataFiles;

    // Owned by the BlockstoreBackedExtentManager::Factory
    AllocState* const _allocState;
};

}  // namespace queryable
}  // namespace mongo
