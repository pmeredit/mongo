/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <chrono>
#include <random>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/mutable_bson/document.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/platform/random.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/checkpoint/file_util.h"
#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/tests/test_utils.h"

using namespace std::chrono_literals;

using namespace mongo;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace {

bool equal(const std::vector<Document>& lhs, const std::vector<const BSONObj*>& rhs) {
    return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), [](const auto& l, auto r) {
        return Document::compare(l, Document{*r}, nullptr) == 0;
    });
}

int rand_range(int min, int max) {
    static thread_local std::mt19937 gen{std::random_device()()};
    using distrib_t = std::uniform_int_distribution<>;
    return distrib_t(min, max)(gen);
}

std::string getString(int size) {
    std::string blah;
    blah.reserve(size);
    for (int i = 0; i < size; i++) {
        blah.push_back((char)rand_range(0, 255));
    }
    return blah;
}

std::vector<BSONObj> objs;

// call just once at start time
void build_objects(int num) {
    invariant(objs.empty());
    objs.reserve(num);
    for (int i = 0; i < num; i++) {
        int recLen = 1;
        int r = rand_range(1, 4);
        if (r == 1) {
            //<1k bytes
            recLen = rand_range(1, 1024);
        } else if (r == 2) {
            // 1k-1MB
            recLen = rand_range(1024, 1024 * 1024);
        } else if (r == 3) {
            // 1MB - 15MB
            recLen = rand_range(1024, 15 * 1024);
        } else {
            // let it stay at 1 byte;
        }
        BSONObjBuilder b;
        std::string data = getString(recLen);
        b.append("data", data);
        objs.push_back(b.obj().copy());
    }
}

const BSONObj* get_object() {
    invariant(!objs.empty());
    return &objs[rand_range(0, objs.size() - 1)];
}

}  // namespace

using namespace streams;
using fspath = std::filesystem::path;

bool oneCheckpointRestore(CheckpointStorage* chkpt,
                          const std::string& streamProcessorId,
                          fspath writeDir,
                          int iter,
                          Context* ctxt) {
    try {
        CheckpointId chkId = chkpt->startCheckpoint();

        std::vector<std::pair<OperatorId, std::vector<const BSONObj*>>> opStates;

        // Upto 1024 operators in a pipeline
        // Upto 6GB total pipeline state
        const size_t maxStateSz = 6_GiB;
        size_t currStateSz = 0;
        int numOperators = rand_range(1, 1024);
        for (int i = 0; i < numOperators && currStateSz < maxStateSz; i++) {
            int opId = i;
            opStates.push_back({opId, std::vector<const BSONObj*>{}});
            // upto 512 BSONObj records per operator
            int numRecs = rand_range(1, 512);
            for (int j = 0; j < numRecs && currStateSz < maxStateSz; j++) {
                opStates.back().second.push_back(get_object());
                currStateSz += opStates.back().second.back()->objsize();
            }

            // get a writer and write it out
            auto writer = chkpt->createStateWriter(chkId, opId);
            for (auto& rec : opStates.back().second) {
                chkpt->appendRecord(writer.get(), Document{*rec});
            }
        }

        chkpt->commitCheckpoint(chkId);
        auto dir = fspath{writeDir / std::to_string(chkId)};
        fspath manifestFile = getManifestFilePath(dir);
        ASSERT_TRUE(std::filesystem::exists(manifestFile));

        // Now begin to read
        auto restoreStorage = std::make_unique<LocalDiskCheckpointStorage>(
            LocalDiskCheckpointStorage::Options{.writeRootDir = writeDir, .restoreRootDir = dir},
            ctxt);
        restoreStorage->startCheckpointRestore(chkId);
        restoreStorage->createCheckpointRestorer(chkId, false);
        for (auto& written : opStates) {
            OperatorId opId = written.first;
            auto reader = restoreStorage->createStateReader(chkId, opId);
            std::vector<Document> retrieved;
            while (boost::optional<Document> rec = chkpt->getNextRecord(reader.get())) {
                retrieved.push_back(rec->getOwned());
            }
            ASSERT_TRUE(equal(retrieved, written.second));
        }

        restoreStorage->checkpointRestored(chkId);

        // Cleanup after ourselves
        std::filesystem::remove_all(dir);

    } catch (const std::exception& e) {
        LOGV2_WARNING(7863427,
                      "Chkpt exception",
                      "spid"_attr = streamProcessorId,
                      "iter"_attr = iter,
                      "msg"_attr = e.what());
        return false;
    }

    return true;
}

int longRunningTest() {

    auto now_millis = mongo::Date_t::now().toMillisSinceEpoch();

    // default run time = 30 mins
    auto run_time = 1800'000ms;
    auto run_until = now_millis + run_time.count();
    int numThreads = 3;

    std::vector<mongo::Promise<void>> promises;
    std::vector<mongo::Future<void>> futures;
    std::vector<mongo::stdx::thread> streamprocs;

    promises.reserve(numThreads);
    futures.reserve(numThreads);

    for (int i = 0; i < numThreads; i++) {
        auto pf = makePromiseFuture<void>();
        promises.push_back(std::move(pf.promise));
        futures.push_back(std::move(pf.future));
    }

    build_objects(32);

    for (int spid = 0; spid < numThreads; spid++) {
        LOGV2_INFO(7863428, "Launching spid", "spid"_attr = spid);

        auto runTest = [spid, run_until]() {
            LocalDiskCheckpointStorage::Options opts{
                .writeRootDir = "/home/ubuntu/chkpoints",
            };

            std::unique_ptr<Context> ctxt{new Context{}};
            ctxt->streamProcessorId = "testStreamProc" + std::to_string(spid);
            ctxt->tenantId = "unit-test";
            std::unique_ptr<CheckpointStorage> chkpt{
                new LocalDiskCheckpointStorage(opts, ctxt.get())};

            for (int iter = 0;; iter++) {
                std::string msg = fmt::format("Beginning spid/iter={}]{}", spid, iter);
                LOGV2_INFO(
                    7863429, "Beginning new spid iter:", "spid"_attr = spid, "iter"_attr = iter);
                ASSERT_TRUE(oneCheckpointRestore(chkpt.get(),
                                                 ctxt->streamProcessorId,
                                                 fspath{opts.writeRootDir},
                                                 iter,
                                                 ctxt.get()));
                auto now_millis = mongo::Date_t::now().toMillisSinceEpoch();
                if (now_millis > run_until) {
                    break;
                }
            }
        };
        mongo::stdx::thread sp{runTest};
        streamprocs.push_back(std::move(sp));
    }

    for (auto& sproc : streamprocs) {
        if (sproc.joinable()) {
            sproc.join();
        }
    }

    LOGV2_INFO(7863430, "All threads ran successfully for:", "run_time"_attr = run_time.count());
    return 1;
}

int nospaceTest() {

    // This is a manual test and so is disabled by default till it is automated

    // The behavior we want to test is that the process does not crash and burn
    // when we run into an out-of-diskspace scenario.
    // Instead, the checkpointing thread (i.e. the "StreamProcessor executor thread" in real life
    // detects this situation and in real life, we might decide to kill the process or just error
    // out this one SP

    // To run it, start this one test, then manually fill up
    // the writeDir so that a subsequent write will run into an out of space issue
    // The intent is that the thread that initiated the write (i.e. a SP) gets errored out (via
    // hasErrored() chkpoint interface call

    // The test will continue trying to start new threads and initiate
    // checkpoint attempts. All these threads will similary get errored
    // out.

    // Subsequently, when space is freed in the writeDir, the next thread
    // to initiate a checkpoint attempt will succeed.

    // Disabled by default
    return 0;

    int spid = 0;
    while (true) {
        ++spid;
        LOGV2_INFO(7863431, "Launching spid", "spid"_attr = spid);

        auto runTest = [spid]() {
            LocalDiskCheckpointStorage::Options opts{
                .writeRootDir = "/tmp",
            };

            std::unique_ptr<Context> ctxt{new Context{}};
            ctxt->streamProcessorId = "testStreamProc" + std::to_string(spid);
            std::unique_ptr<CheckpointStorage> chkpt{
                new LocalDiskCheckpointStorage(opts, ctxt.get())};

            for (int iter = 0;; iter++) {
                std::string msg = fmt::format("Beginning spid/iter={}]{}", spid, iter);
                LOGV2_INFO(
                    7863432, "Beginning new spid iter:", "spid"_attr = spid, "iter"_attr = iter);
                if (!oneCheckpointRestore(chkpt.get(),
                                          ctxt->streamProcessorId,
                                          fspath{opts.writeRootDir},
                                          iter,
                                          ctxt.get())) {
                    break;
                }
            }
        };

        mongo::stdx::thread sp{runTest};
        if (sp.joinable()) {
            sp.join();
        }

        sleep(1);
    }

    return 1;
}

int main(int argc, char** argv) {

    auto help = []() {
        std::cerr << "Usage: prog -l[longRunning] | -n[nospace] | -h[help]" << std::endl;
    };

    if (argc != 2) {
        help();
        return 0;
    }

    if (std::string(argv[1]) == "-l") {
        return longRunningTest();
    } else if (std::string(argv[1]) == "-n") {
        return nospaceTest();
    } else {
        help();
        return 0;
    }
}
