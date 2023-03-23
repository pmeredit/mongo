/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/logv2/log.h"

#include "streams/exec/fake_kafka_partition_consumer.h"

namespace streams {

using namespace mongo;

void FakeKafkaPartitionConsumer::addDocuments(std::vector<KafkaSourceDocument> docs) {
    stdx::lock_guard<Latch> lock(_mutex);
    _docs.push(std::move(docs));
}

std::vector<KafkaSourceDocument> FakeKafkaPartitionConsumer::doGetDocuments() {
    stdx::lock_guard<Latch> lock(_mutex);
    if (_docs.empty()) {
        return {};
    }
    auto docs = std::move(_docs.front());
    _docs.pop();
    return docs;
}

}  // namespace streams
