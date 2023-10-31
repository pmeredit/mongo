#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/util/uuid.h"
#include "streams/exec/context.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/stages_gen.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

class MetricManager;

std::unique_ptr<Context> getTestContext(mongo::ServiceContext* svcCtx,
                                        MetricManager* metricManager,
                                        std::string tenantId = "",
                                        std::string streamProcessorId = "");

mongo::BSONObj getTestLogSinkSpec();

mongo::BSONObj getTestMemorySinkSpec();

mongo::BSONObj getTestSourceSpec();

std::vector<mongo::BSONObj> parseBsonVector(std::string json);

// Returns a connections map with a Kafka where isTest = true.
mongo::stdx::unordered_map<std::string, mongo::Connection> testKafkaConnectionRegistry();

// Returns a connections map with an in-memory source connection.
mongo::stdx::unordered_map<std::string, mongo::Connection> testInMemoryConnectionRegistry();

// Returns a $source syntax BSONObj that will use a KafkaConsumer with FakeKafkaPartitionConsumers.
mongo::BSONObj testKafkaSourceSpec(int partitionCount = 1);

// Creates a test checkpoint storage instance. If the environment variable
// CHECKPOINT_TEST_MONGODB_URI is set, this will create a real MongoDBCheckpointStorage instance
// using that URI. command line example using the environment variable:
//  CHECKPOINT_TEST_MONGODB_URI=mongodb://localhost && ninja +streams_checkpoint_storage_test -j400
std::unique_ptr<OldCheckpointStorage> makeCheckpointStorage(
    mongo::ServiceContext* serviceContext,
    Context* context,
    const std::string& collection = "checkpointCollection",
    const std::string& database = "test");

// Returns a cloned BSON object with the metadata fields removed (e.g. `_ts` and
// `_stream_meta`) for easier comparison checks.
mongo::BSONObj sanitizeDoc(const mongo::BSONObj& obj);

std::shared_ptr<MongoDBProcessInterface> makeMongoDBProcessInterface(
    mongo::ServiceContext* serviceContext,
    const std::string& uri,
    const std::string& database,
    const std::string& collection);

// returns the number of dlq docs in all the operators in the dag
size_t getNumDlqDocsFromOperatorDag(const OperatorDag& dag);

}  // namespace streams
