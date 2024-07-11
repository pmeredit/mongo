/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <queue>
#include <string>
#include <vector>

#include "mongo/platform/basic.h"
#include "mongo/platform/mutex.h"
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_stats.h"

namespace streams {

/**
 * Fake implementation of KafkaPartitionConsumer for test-only purposes.
 * This class is thread-safe.
 */
class FakeKafkaPartitionConsumer : public KafkaPartitionConsumerBase {
public:
    FakeKafkaPartitionConsumer(Options options)
        : KafkaPartitionConsumerBase(std::move(options)),
          _docsPerChunk(_options.maxNumDocsToReturn) {}

    FakeKafkaPartitionConsumer() : KafkaPartitionConsumerBase(Options{}) {}

    // Adds a batch of documents that will be returned together when getDocuments() is called.
    void addDocuments(std::vector<KafkaSourceDocument> docs);

private:
    // friend class so test code can change _overrideOffsets, _internalOffset, and _docsPerChunk.
    friend class CheckpointTestWorkload;
    friend class KafkaConsumerOperatorTest;
    friend class WindowOperatorTest;

    void doInit() override {}
    void doStart() override;
    void doStop() override {}
    ConnectionStatus doGetConnectionStatus() const override {
        return ConnectionStatus{ConnectionStatus::Status::kConnected};
    }
    boost::optional<int64_t> doGetStartOffset() const override {
        return _startOffset;
    }

    boost::optional<int64_t> doGetLatestOffsetAtBroker() const override {
        return boost::none;
    }

    boost::optional<int64_t> doGetNumPartitions() const override {
        MONGO_UNREACHABLE;
    }

    // Returns the next batch of documents from _docs, if any available.
    std::vector<KafkaSourceDocument> doGetDocuments() override;

    OperatorStats doGetStats() override;

    // Guards _docs.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("FakeKafkaPartitionConsumer::mutex");
    std::vector<KafkaSourceDocument> _docs;

    // If true, the document offsets returned use the internal index.
    bool _overrideOffsets{true};
    int64_t _startOffset{0};
    int64_t _currentOffset{0};
    int _docsPerChunk{std::numeric_limits<int32_t>::max()};
};

}  // namespace streams
