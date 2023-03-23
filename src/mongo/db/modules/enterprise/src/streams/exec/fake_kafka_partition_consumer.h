#pragma once

#include <queue>
#include <string>
#include <vector>

#include "mongo/platform/basic.h"
#include "mongo/platform/mutex.h"
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * Fake implementation of KafkaPartitionConsumer for test-only purposes.
 * This class is thread-safe.
 */
class FakeKafkaPartitionConsumer : public KafkaPartitionConsumerBase {
public:
    // Adds a batch of documents that will be returned together when getDocuments() is called.
    void addDocuments(std::vector<KafkaSourceDocument> docs);

private:
    void doInit() override {}
    void doStart() override {}
    void doStop() override {}

    // Returns the next batch of documents from _docs, if any available.
    std::vector<KafkaSourceDocument> doGetDocuments() override;

    // Guards _docs.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("FakeKafkaPartitionConsumer::mutex");
    std::queue<std::vector<KafkaSourceDocument>> _docs;
};

}  // namespace streams
