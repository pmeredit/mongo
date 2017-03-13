/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "queryable_evictor.h"

#include "mongo/db/client.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/lock_manager_defs.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/exit.h"

#include "queryable_alloc_state.h"
#include "queryable_datafile.h"

namespace mongo {
namespace queryable {

namespace {
const std::size_t kSleepTimeSeconds = 1;
}  // namespace

void evict(AllocState* const allocState) {
    if (allocState->getMemoryAllocated() < allocState->getMemoryQuotaBytes()) {
        return;
    }

    auto opCtx = cc().makeOperationContext();
    Lock::GlobalWrite writeLock(opCtx.get());

    const std::size_t pagesAlloced = allocState->getNumPagesAllocated();
    // Evict half the pages, chosen arbitrarily.
    const std::size_t toEvict = pagesAlloced / 2U;
    for (std::size_t idx = 0; idx < toEvict; ++idx) {
        DataFile* dataFile;
        std::size_t pageIdx;

        allocState->selectPageForFree(&dataFile, &pageIdx);
        fassertStatusOK(22017, dataFile->releasePage(pageIdx));
    }
}

void startQueryableEvictorThread(AllocState* const allocState) {
    stdx::thread([allocState] {
        Client::initThread("QueryableMMapEvictor");

        while (!globalInShutdownDeprecated()) {
            sleepsecs(kSleepTimeSeconds);
            evict(allocState);
        }
    }).detach();
}

}  // namespace queryable
}  // namespace mongo
