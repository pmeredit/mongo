/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/mutable_bson/document.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/unittest/unittest.h"
#include "streams/exec/checkpoint/file_util.h"
#include "streams/exec/checkpoint/local_disk_checkpoint_storage.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/tests/test_utils.h"
#include <vector>

using namespace mongo;
using fspath = std::filesystem::path;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace {

bool equal(const std::vector<Document>& lhs, const std::vector<Document>& rhs) {
    return std::equal(
        lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), [](const auto& l, const auto& r) {
            return Document::compare(l, r, nullptr) == 0;
        });
}

bool equal(const std::vector<Document>& lhs, const std::vector<BSONObj>& rhs) {
    return std::equal(
        lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), [](const auto& l, const auto& r) {
            return Document::compare(l, Document{r}, nullptr) == 0;
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

}  // namespace

namespace streams {

class LocalDiskCheckpointStorageTest : public AggregationContextFixture {
protected:
    std::tuple<std::unique_ptr<Context>,
               std::unique_ptr<Executor>,
               std::unique_ptr<CheckpointStorage>>
    makeLocalDiskCheckpointStorage(std::string streamProcessorId,
                                   LocalDiskCheckpointStorage::Options opts) {
        mongo::ServiceContext* svcCtx = getGlobalServiceContext();
        std::string tenantId = "unit-test";
        std::string processorId = streamProcessorId;
        std::unique_ptr<Context> ctxt;
        std::unique_ptr<Executor> executor;
        std::tie(ctxt, executor) = getTestContext(svcCtx, tenantId, processorId);

        std::unique_ptr<CheckpointStorage> chkpt{
            (CheckpointStorage*)new LocalDiskCheckpointStorage(opts, ctxt.get())};
        chkpt->registerMetrics(executor->getMetricManager());

        return {std::move(ctxt), std::move(executor), std::move(chkpt)};
    }
};

// Writes a checkpoint with two operators, reads it back in and tests that the read state is as
// expected. State is small enough to fit in one state file
TEST_F(LocalDiskCheckpointStorageTest, basic_round_trip) {
    LocalDiskCheckpointStorage::Options opts{
        .writeRootDir = "/tmp",
    };

    std::string streamProcessorId = "testStreamProc_basic_round_trip";

    std::unique_ptr<Context> ctxt;
    std::unique_ptr<Executor> executor;
    std::unique_ptr<CheckpointStorage> chkpt;

    std::tie(ctxt, executor, chkpt) = makeLocalDiskCheckpointStorage(streamProcessorId, opts);

    CheckpointId chkId = chkpt->startCheckpoint();

    std::vector<Document> op1State = {
        Document{
            fromjson(R"({"id": 2, "timestamp": "2023-04-10T17:02:20.062839", "operator": 0})")},
        Document{
            fromjson(R"({"id": 3, "timestamp": "2023-04-10T17:03:20.062839", "operator": 0})")},
    };

    std::vector<Document> op2State = {
        Document{
            fromjson(R"({"id": 12, "timestamp": "2023-04-10T17:02:20.062839", "operator": 1})")},
        Document{
            fromjson(R"({"id": 13, "timestamp": "2023-04-10T17:03:20.062839", "operator": 1})")},
        Document{
            fromjson(R"({"id": 14, "timestamp": "2023-04-10T17:03:20.062839", "operator": 1})")},
    };

    {
        auto writer1 = chkpt->createStateWriter(chkId, 1);
        for (auto& rec : op1State) {
            chkpt->appendRecord(writer1.get(), rec);
        }
    }

    {
        auto writer2 = chkpt->createStateWriter(chkId, 2);
        for (auto& rec : op2State) {
            chkpt->appendRecord(writer2.get(), rec);
        }
    }

    // Wait till checkpoint is written
    chkpt->commitCheckpoint(chkId);
    auto dir = fspath{opts.writeRootDir / std::to_string(chkId)};
    fspath manifestFile = getManifestFilePath(dir);
    ASSERT_TRUE(std::filesystem::exists(manifestFile));

    chkpt.reset();
    executor.reset();
    ctxt.reset();

    // Now begin to read
    opts.restoreRootDir = dir;
    std::tie(ctxt, executor, chkpt) = makeLocalDiskCheckpointStorage(streamProcessorId, opts);
    auto restoredChkId = chkpt->getRestoreCheckpointId();
    ASSERT_TRUE(restoredChkId);
    ASSERT_EQ(chkId, *restoredChkId);
    chkpt->startCheckpointRestore(chkId);
    chkpt->createCheckpointRestorer(chkId, false);

    {
        auto reader1 = chkpt->createStateReader(chkId, 1);
        std::vector<Document> retrievedOp1State;
        while (boost::optional<mongo::Document> rec = chkpt->getNextRecord(reader1.get())) {
            retrievedOp1State.push_back(rec->getOwned());
        }
        ASSERT_TRUE(equal(op1State, retrievedOp1State));
    }

    {
        auto reader2 = chkpt->createStateReader(chkId, 2);
        std::vector<Document> retrievedOp2State;
        while (boost::optional<mongo::Document> rec = chkpt->getNextRecord(reader2.get())) {
            retrievedOp2State.push_back(rec->getOwned());
        }
        ASSERT_TRUE(equal(op2State, retrievedOp2State));
    }

    chkpt->checkpointRestored(chkId);

    // Cleanup
    std::filesystem::remove_all(dir);
}

// Checkpoint with many more operators, opIds are non-contiguous
// Each operator has upto 100 records. Each record is upto 5'MB
// Will typically create 15-20 state files where each file is 64'MB
TEST_F(LocalDiskCheckpointStorageTest, many_operators) {

    LocalDiskCheckpointStorage::Options opts{
        .writeRootDir = "/tmp",
    };

    std::string streamProcessorId = "testStreamProc_many_operators";
    std::unique_ptr<Context> ctxt;
    std::unique_ptr<Executor> executor;
    std::unique_ptr<CheckpointStorage> chkpt;

    std::tie(ctxt, executor, chkpt) = makeLocalDiskCheckpointStorage(streamProcessorId, opts);

    CheckpointId chkId = chkpt->startCheckpoint();

    std::vector<std::pair<OperatorId, std::vector<BSONObj>>> opStates;
    for (int opId : {1, 3, 4, 5, 6, 8, 10, 29, 30, 31, 32, 33, 34}) {
        opStates.push_back({opId, std::vector<BSONObj>{}});
        int numRecs = rand_range(1, 100);
        for (int i = 0; i < numRecs; i++) {
            BSONObjBuilder b;
            b.appendNumber("opid", opId);
            std::string data = getString(rand_range(1, 5'000'000));
            b.append("data", data);
            opStates.back().second.push_back(b.obj().copy());
        }

        // get a writer and write it out
        auto writer = chkpt->createStateWriter(chkId, opId);
        for (auto& rec : opStates.back().second) {
            chkpt->appendRecord(writer.get(), Document{rec});
        }
    }

    // commit
    chkpt->commitCheckpoint(chkId);

    // Wait till checkpoint is written
    auto dir = fspath{opts.writeRootDir / std::to_string(chkId)};
    fspath manifestFile = getManifestFilePath(dir);
    ASSERT_TRUE(std::filesystem::exists(manifestFile));

    chkpt.reset();
    executor.reset();
    ctxt.reset();

    opts.restoreRootDir = dir;
    std::tie(ctxt, executor, chkpt) = makeLocalDiskCheckpointStorage(streamProcessorId, opts);
    auto restoredChkId = chkpt->getRestoreCheckpointId();
    ASSERT_TRUE(restoredChkId);
    ASSERT_EQ(chkId, *restoredChkId);
    chkpt->startCheckpointRestore(chkId);
    chkpt->createCheckpointRestorer(chkId, false);
    for (auto& written : opStates) {
        OperatorId opId = written.first;
        auto reader = chkpt->createStateReader(chkId, opId);
        std::vector<Document> retrieved;
        while (boost::optional<Document> rec = chkpt->getNextRecord(reader.get())) {
            retrieved.push_back(rec->getOwned());
        }
        ASSERT_TRUE(equal(retrieved, written.second));
    }

    chkpt->checkpointRestored(chkId);
    // Cleanup
    std::filesystem::remove_all(dir);
}

// Writes a large checkpoint with 3 operators and ~400MB spread out over 4-5 state files.
// One operator state spans across multiple files and an individual BSON record also
// spans across two files

// This is done from three threads simultaneously to test multiple stream processors
// Note that this test does not use the same fixture as the previous tests
TEST_F(LocalDiskCheckpointStorageTest, large_round_trip_multi_threads) {
    int numThreads = 3;
    std::vector<mongo::Promise<void>> promises;
    std::vector<mongo::Future<void>> futures;
    std::vector<mongo::stdx::thread> streamprocs;

    promises.reserve(numThreads);
    futures.reserve(numThreads);
    streamprocs.reserve(numThreads);

    for (int i = 0; i < numThreads; i++) {
        auto pf = makePromiseFuture<void>();
        promises.push_back(std::move(pf.promise));
        futures.push_back(std::move(pf.future));
    }

    for (int spid = 0; spid < numThreads; spid++) {

        auto runTest = [this, spid, &promises]() {
            try {
                std::string streamProcessorId = "testStreamProc" + std::to_string(spid);
                LocalDiskCheckpointStorage::Options opts{
                    .writeRootDir = fmt::format("/tmp/{}", streamProcessorId),
                };

                std::unique_ptr<Context> ctxt;
                std::unique_ptr<Executor> executor;
                std::unique_ptr<CheckpointStorage> chkpt;
                std::tie(ctxt, executor, chkpt) =
                    makeLocalDiskCheckpointStorage(streamProcessorId, opts);

                CheckpointId chkId = chkpt->startCheckpoint();

                std::vector<std::pair<OperatorId, std::vector<BSONObj>>> opStates;
                std::vector<int> opIds{1, 2, 3};
                std::vector<int> opRecs{10, 3, 5};
                for (size_t i = 0; i < opIds.size(); i++) {
                    int opId = opIds[i];
                    opStates.push_back({opId, std::vector<BSONObj>{}});
                    int numRecs = opRecs[i];
                    for (int j = 0; j < numRecs; j++) {
                        BSONObjBuilder b;
                        b.appendNumber("opid", opId);
                        // add spid to distinguish data being generated in different threads
                        b.appendNumber("spid", spid);
                        std::string data = getString(15_MiB);
                        b.append("data", data);
                        opStates.back().second.push_back(b.obj().copy());
                    }

                    // get a writer and write it out
                    auto writer = chkpt->createStateWriter(chkId, opId);
                    for (auto& rec : opStates.back().second) {
                        chkpt->appendRecord(writer.get(), Document{rec});
                    }
                }

                auto dir = fspath{opts.writeRootDir / std::to_string(chkId)};
                chkpt->commitCheckpoint(chkId);
                // wait till manifest file is written
                fspath manifestFile = getManifestFilePath(fspath{dir});
                ASSERT_TRUE(std::filesystem::exists(manifestFile));

                chkpt.reset();
                executor.reset();
                ctxt.reset();

                // Now begin to read
                opts.restoreRootDir = dir;
                std::tie(ctxt, executor, chkpt) =
                    makeLocalDiskCheckpointStorage(streamProcessorId, opts);
                auto restoredChkId = chkpt->getRestoreCheckpointId();
                ASSERT_TRUE(restoredChkId);
                ASSERT_EQ(chkId, *restoredChkId);
                chkpt->startCheckpointRestore(chkId);
                chkpt->createCheckpointRestorer(chkId, false);

                for (auto& written : opStates) {
                    OperatorId opId = written.first;
                    auto reader = chkpt->createStateReader(chkId, opId);
                    std::vector<Document> retrieved;
                    while (boost::optional<Document> rec = chkpt->getNextRecord(reader.get())) {
                        retrieved.push_back(rec->getOwned());
                    }
                    ASSERT_TRUE(equal(retrieved, written.second));
                }

                chkpt->checkpointRestored(chkId);

                // Cleanup
                std::filesystem::remove_all(dir);
            } catch (const std::exception& e) {
                // Signal failure
                promises[spid].setError(Status{ErrorCodes::Error::OperationFailed, e.what()});
                return;
            }

            // Signal success
            promises[spid].emplaceValue();
        };

        mongo::stdx::thread sp{runTest};
        streamprocs.push_back(std::move(sp));
    }

    // wait on the sp results
    try {
        for (int i = 0; i < numThreads; i++) {
            futures[i].get();
        }
    } catch (const std::exception& e) {
        LOGV2_WARNING(7863424, "Test failed due to exception - ", "exception"_attr = e.what());
        ASSERT_TRUE(false);
    }

    for (int i = 0; i < numThreads; i++) {
        if (streamprocs[i].joinable()) {
            streamprocs[i].join();
        }
    }
}

}  // namespace streams
