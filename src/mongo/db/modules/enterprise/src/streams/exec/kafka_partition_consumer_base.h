/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <rdkafkacpp.h>
#include <string>
#include <vector>

#include "streams/exec/connection_status.h"
#include "streams/exec/context.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/source_buffer_manager.h"

namespace streams {

class EventDeserializer;

/**
 * The abstract base class of classes used to tail documents from a partition of a Kafka topic.
 */
class KafkaPartitionConsumerBase {
public:
    struct Options {
        // List of bootstrap servers to specify in Kafka's bootstrap.servers configuration
        // parameter.
        std::string bootstrapServers;
        // Name of the topic to tail.
        std::string topicName;
        // Partition of the topic to tail.
        int32_t partition{0};
        // Start offset in the partition to start tailing from.
        int64_t startOffset{RdKafka::Topic::OFFSET_BEGINNING};
        // EventDeserializer to use to deserialize Kafka messages to mongo::Documents.
        EventDeserializer* deserializer{nullptr};
        // Maximum number of documents getDocuments() should return per call.
        int32_t maxNumDocsToReturn{500};
        // Auth related config options like "sasl.username".
        mongo::stdx::unordered_map<std::string, std::string> authConfig;
        // Sleep duration after Kafka api calls fail.
        mongo::stdx::chrono::milliseconds kafkaRequestFailureSleepDurationMs{1'000};
        // Metrics that track the number of docs and bytes prefetched.
        std::shared_ptr<IntGauge> queueSizeGauge;
        std::shared_ptr<IntGauge> queueByteSizeGauge;
        // GWProxy endpoint hostname or IP address.
        boost::optional<std::string> gwproxyEndpoint;
        // GWProxy symmetric encryption key.
        boost::optional<std::string> gwproxyKey;
        // librdkafka's queued.max.messages.kbytes setting.
        boost::optional<int64_t> rdkafkaQueuedMaxMessagesKBytes;
    };

    KafkaPartitionConsumerBase(Context* context, Options options);

    virtual ~KafkaPartitionConsumerBase();

    // Initializes internal state.
    // Throws an exception if any error is encountered during the initialization.
    void init() {
        doInit();
    }

    // Starts the consumer. Returns the starting log offset.
    void start() {
        doStart();
    }

    // Stops the consumer.
    void stop() {
        doStop();
    }

    // Whether the consumer is connected to the source Kafka cluster.
    ConnectionStatus getConnectionStatus() const {
        return doGetConnectionStatus();
    }

    // Returns the initial offset used to start tailing the Kafka partition.
    // Returns boost::none if the start offset has not been initialized yet.
    boost::optional<int64_t> getStartOffset() const {
        return doGetStartOffset();
    }

    boost::optional<int64_t> getLatestOffsetAtBroker() const {
        return doGetLatestOffsetAtBroker();
    }

    // Returns the next batch of documents tailed from the partition, if any available.
    std::vector<KafkaSourceDocument> getDocuments() {
        return doGetDocuments();
    }

    OperatorStats getStats() {
        return doGetStats();
    }

    std::string topicName() const {
        return _options.topicName;
    }

    int32_t partition() const {
        return _options.partition;
    }

protected:
    virtual void doInit() = 0;
    virtual void doStart() = 0;
    virtual void doStop() = 0;
    virtual ConnectionStatus doGetConnectionStatus() const = 0;
    virtual boost::optional<int64_t> doGetStartOffset() const = 0;
    virtual boost::optional<int64_t> doGetLatestOffsetAtBroker() const = 0;
    virtual std::vector<KafkaSourceDocument> doGetDocuments() = 0;
    virtual OperatorStats doGetStats() = 0;

    Context* _context{nullptr};
    const Options _options;
    SourceBufferManager::SourceBufferHandle _sourceBufferHandle;
};

}  // namespace streams
