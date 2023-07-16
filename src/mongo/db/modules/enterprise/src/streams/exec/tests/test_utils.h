#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/context.h"
#include "streams/exec/stages_gen.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

class MetricManager;

std::unique_ptr<Context> getTestContext(mongo::ServiceContext* svcCtx,
                                        MetricManager* metricManager);

mongo::BSONObj getTestLogSinkSpec();

mongo::BSONObj getTestMemorySinkSpec();

mongo::BSONObj getTestSourceSpec();

std::vector<mongo::BSONObj> parseBsonVector(std::string json);

// Returns a connections map with a Kafka where isTest = true.
mongo::stdx::unordered_map<std::string, mongo::Connection> testKafkaConnectionRegistry();

// Returns a $source syntax BSONObj that will use a KafkaConsumer with FakeKafkaPartitionConsumers.
mongo::BSONObj testKafkaSourceSpec(int partitionCount = 1);

// Creates a test checkpoint storage instance. If the environment variable
// CHECKPOINT_TEST_MONGODB_URI is set, this will create a real CheckpointMongoDBStorage instance
// using that URI. command line example using the environment variable:
//  CHECKPOINT_TEST_MONGODB_URI=mongodb://localhost && ninja +streams_checkpoint_storage_test -j400
std::unique_ptr<CheckpointStorage> makeCheckpointStorage(
    mongo::ServiceContext* serviceContext,
    const std::string& collection = "checkpointCollection",
    const std::string& database = "test");

}  // namespace streams
