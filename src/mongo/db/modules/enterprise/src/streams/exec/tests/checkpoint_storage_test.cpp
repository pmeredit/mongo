/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/assert.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/mongodb_checkpoint_storage.h"
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/tests/test_utils.h"

using namespace mongo;

namespace streams {

namespace {
void testBasicIdAndCommitLogic(CheckpointStorage* storage) {
    // Validate there is no latest checkpointId.
    ASSERT(!storage->readLatestCheckpointId());
    // Create an ID, but don't commit it.
    auto id = storage->createCheckpointId();
    // Validate there is still no latest committed checkpoint.
    ASSERT(!storage->readLatestCheckpointId());
    // Commit and validate readLatest returns it.
    storage->commit(id);
    ASSERT_EQ(id, storage->readLatestCheckpointId());
    // Create 100 empty checkpoints, commit them, validate the most recent is returned.
    std::vector<CheckpointId> ids;
    auto lastId = id;
    for (int i = 0; i < 100; ++i) {
        auto id = storage->createCheckpointId();
        ASSERT_EQ(lastId, *storage->readLatestCheckpointId());
        storage->commit(id);
        ASSERT_EQ(id, storage->readLatestCheckpointId());
        ids.push_back(id);
        lastId = id;
    }
}
}  // namespace

class CheckpointStorageTest : public AggregationContextFixture {
protected:
    auto makeStorage(std::string tenantId = UUID::gen().toString(),
                     std::string streamProcessorId = UUID::gen().toString()) {
        std::string collectionName(UUID::gen().toString());
        std::string dbName("test");
        return makeCheckpointStorage(_serviceContext, tenantId, streamProcessorId);
    }

    QueryTestServiceContext _qtServiceContext;
    ServiceContext* _serviceContext{_qtServiceContext.getServiceContext()};
    bool _useRealMongo{false};
    std::string _mongodbUri;
    const std::string _database{"test"};
};

TEST_F(CheckpointStorageTest, BasicIdAndCommitLogic) {
    auto storage = makeStorage();
    testBasicIdAndCommitLogic(storage.get());
}

TEST_F(CheckpointStorageTest, BasicOperatorState) {
    auto innerTest = [&](uint32_t numOperators, uint32_t chunksPerOperator) {
        auto storage = makeStorage();
        auto id = storage->createCheckpointId();
        stdx::unordered_map<OperatorId, std::vector<BSONObj>> expectedState;
        for (OperatorId operatorId = 0; size_t(operatorId) < numOperators; ++operatorId) {
            expectedState.emplace(operatorId, std::vector<BSONObj>{});
            for (uint32_t chunk = 0; chunk < chunksPerOperator; ++chunk) {
                expectedState[operatorId].push_back(
                    BSON("a" << UUID::gen().toString() << "b" << (int64_t)chunk << "_id" << 0));
                storage->addState(id, operatorId, expectedState[operatorId].back(), chunk);
            }
        }
        storage->commit(id);
        for (uint32_t operatorId = 0; operatorId < numOperators; ++operatorId) {
            for (int32_t chunkNumber = 0; size_t(chunkNumber) < chunksPerOperator; ++chunkNumber) {
                auto state = storage->readState(id, operatorId, chunkNumber);
                ASSERT(state);
                ASSERT_BSONOBJ_EQ(expectedState[operatorId][chunkNumber], *state);
            }
        }
    };

    innerTest(10, 1);
    innerTest(1, 10);
    innerTest(10, 10);
}

TEST_F(CheckpointStorageTest, BasicMultipleProcessors) {
    int countThreads = 20;
    std::vector<stdx::thread> threads;
    auto tenantId = UUID::gen().toString();
    for (int i = 0; i < countThreads; ++i) {
        threads.emplace_back([this, i, tenantId]() {
            std::string streamProcessorId(i, 'a');
            auto storage = makeStorage(tenantId, streamProcessorId);
            testBasicIdAndCommitLogic(storage.get());
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}

}  // namespace streams
