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

}  // namespace streams
