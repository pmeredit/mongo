/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/bson/bsonobj.h"
#include "mongo/util/uuid.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/message.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/stages_gen.h"

namespace mongo {
class ConcurrentMemoryAggregator;
class ServiceContext;
}  // namespace mongo

namespace streams {

// Validations of stats().stateSize should use this helper function.
void assertStateSize(int64_t expected, int64_t actual);

class MetricManager;

// Test class to help with registering metrics
class OperatorDagTest {
public:
    void registerMetrics(OperatorDag* dag, MetricManager* metricManager) {
        for (auto& oper : dag->_operators) {
            oper->registerMetrics(metricManager);
        }
    }
};

std::tuple<std::unique_ptr<Context>, std::unique_ptr<Executor>> getTestContext(
    mongo::ServiceContext* svcCtx,
    std::string tenantId = "",
    std::string streamProcessorId = "",
    mongo::ConcurrentMemoryAggregator* memoryAggregator = nullptr);

mongo::BSONObj getTestLogSinkSpec();

mongo::BSONObj getTestMemorySinkSpec();

mongo::BSONObj getNoOpSinkSpec();

mongo::BSONObj getTestSourceSpec();

std::vector<mongo::BSONObj> parseBsonVector(std::string json);

// Returns a connections map with a Kafka where isTest = true.
mongo::stdx::unordered_map<std::string, mongo::Connection> testKafkaConnectionRegistry();

// Returns a connections map with an in-memory source connection.
mongo::stdx::unordered_map<std::string, mongo::Connection> testInMemoryConnectionRegistry();

// Returns a $source syntax BSONObj that will use a KafkaConsumer with FakeKafkaPartitionConsumers.
mongo::BSONObj testKafkaSourceSpec(int partitionCount = 1);

// Returns a cloned BSON object with the metadata fields removed (e.g. `_ts` and
// `_stream_meta`) for easier comparison checks.
mongo::BSONObj sanitizeDoc(const mongo::BSONObj& obj);

std::shared_ptr<MongoDBProcessInterface> makeMongoDBProcessInterface(
    mongo::ServiceContext* serviceContext,
    const std::string& uri,
    const std::string& database,
    const std::string& collection);

std::shared_ptr<OperatorDag> makeDagFromBson(const std::vector<mongo::BSONObj>& bsonPipeline,
                                             std::unique_ptr<Context>& context,
                                             std::unique_ptr<Executor>& executor,
                                             OperatorDagTest& dagTest);


// returns the number of dlq docs in all the operators in the dag
size_t getNumDlqDocsFromOperatorDag(const OperatorDag& dag);

// returns the number of bytes sent to dlq in all the operators in the dag
size_t getNumDlqBytesFromOperatorDag(const OperatorDag& dag);

// convert a queue of StreamMsgUnion into a vector
std::vector<StreamMsgUnion> queueToVector(std::deque<StreamMsgUnion> queue);

class TestMetricsVisitor {
public:
    const auto& counters() {
        return _counters;
    }

    const auto& gauges() {
        return _gauges;
    }

    const auto& intGauges() {
        return _intGauges;
    }

    const auto& callbackGauges() {
        return _callbackGauges;
    }

    void visit(Counter* counter,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _counters[getProcessorIdLabel(labels)][name] = counter;
    }

    void visit(Gauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _gauges[getProcessorIdLabel(labels)][name] = gauge;
    }

    void visit(IntGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _intGauges[getProcessorIdLabel(labels)][name] = gauge;
    }

    void visit(CallbackGauge* gauge,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {
        _callbackGauges[getProcessorIdLabel(labels)][name] = gauge;
    }

    void visit(Histogram* histogram,
               const std::string& name,
               const std::string& description,
               const MetricManager::LabelsVec& labels) {}

private:
    std::string getProcessorIdLabel(const MetricManager::LabelsVec& labels) {
        auto result = std::find_if(labels.begin(), labels.end(), [](const auto& l) {
            return l.first == kProcessorIdLabelKey;
        });
        invariant(result != labels.end());
        return result->second;
    }

    using ProcessorId = std::string;
    using MetricName = std::string;
    mongo::stdx::unordered_map<ProcessorId, mongo::stdx::unordered_map<MetricName, Counter*>>
        _counters;
    mongo::stdx::unordered_map<ProcessorId, mongo::stdx::unordered_map<MetricName, Gauge*>> _gauges;
    mongo::stdx::unordered_map<ProcessorId, mongo::stdx::unordered_map<MetricName, IntGauge*>>
        _intGauges;
    mongo::stdx::unordered_map<ProcessorId, mongo::stdx::unordered_map<MetricName, CallbackGauge*>>
        _callbackGauges;
};

}  // namespace streams
