/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/fake_kafka_partition_consumer.h"

#include <rdkafkacpp.h>

#include "mongo/logv2/log.h"
#include "streams/exec/message.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

void FakeKafkaPartitionConsumer::addDocuments(std::vector<KafkaSourceDocument> docs) {
    stdx::lock_guard<Latch> lock(_mutex);
    _docs.insert(
        _docs.end(), std::make_move_iterator(docs.begin()), std::make_move_iterator(docs.end()));
}

void FakeKafkaPartitionConsumer::doStart() {
    _currentOffset = _options.startOffset;
    if (_options.startOffset == RdKafka::Topic::OFFSET_BEGINNING ||
        _options.startOffset == RdKafka::Topic::OFFSET_END) {
        // Note: we start at index 0 even for OFFSET_END. We can change this when a test
        // needs it.
        _currentOffset = 0;
    }
}

std::vector<KafkaSourceDocument> FakeKafkaPartitionConsumer::doGetDocuments() {
    stdx::lock_guard<Latch> lock(_mutex);

    std::vector<KafkaSourceDocument> results;
    results.reserve(std::min(_docs.size() - _currentOffset, size_t(_docsPerChunk)));
    while (size_t(_currentOffset) < _docs.size()) {
        auto doc = _docs[_currentOffset];
        if (_overrideOffsets) {
            doc.offset = _currentOffset;
            doc.partition = _options.partition;
        }  // Else, will use the partition and offset supplied in addDocuments.

        _currentOffset++;
        results.push_back(std::move(doc));

        // If docsPerChunk is set and the results.size() exceeds it, return.
        if (results.size() >= size_t(_docsPerChunk)) {
            break;
        }
    }

    return results;
}

}  // namespace streams
