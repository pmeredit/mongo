#pragma once

#include <rdkafkacpp.h>
#include <string>
#include <vector>

#include "mongo/util/duration.h"
#include "streams/exec/message.h"

namespace streams {

class EventDeserializer;

/**
 * The abstract base class of classes used to tail documents from a partition of a Kafka topic.
 */
class KafkaPartitionConsumerBase {
public:
    struct KafkaOptions {
        // timeout_ms to use with RdKafka::Consumer::consume_callback.
        int32_t kafkaConsumeCallbackTimeoutMs{5000};
    };

    struct Options {
        KafkaOptions kafkaOptions{};
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
        // Maximum number of documents this consumer should prefetch and have ready for the caller
        // to retrieve via getDocuments().
        // Note that we do not honor this limit strictly and we exceed this limit by at least
        // maxNumDocsToReturn depending upon how many documents consume_callback() returns in a
        // single call.
        int32_t maxNumDocsToPrefetch{500 * 10};
        // Auth related config options like "sasl.username".
        mongo::stdx::unordered_map<std::string, std::string> authConfig;
        // Timeout used for remote calls to Kafka like calling query_watermark_offsets during start.
        int32_t kafkaRequestTimeoutMs{mongo::Milliseconds(mongo::Minutes(1)).count()};
    };

    KafkaPartitionConsumerBase(Options options) : _options(std::move(options)) {}

    virtual ~KafkaPartitionConsumerBase() = default;

    // Initializes internal state.
    // Throws an exception if any error is encountered during the initialization.
    virtual void init() {
        doInit();
    }

    // Starts the consumer. Returns the starting log offset.
    virtual int64_t start() {
        return doStart();
    }

    // Stops the consumer.
    virtual void stop() {
        doStop();
    }

    // Returns the next batch of documents tailed from the partition, if any available.
    virtual std::vector<KafkaSourceDocument> getDocuments() {
        return doGetDocuments();
    }

protected:
    virtual void doInit() = 0;

    // Returns the starting log offset.
    virtual int64_t doStart() = 0;

    virtual void doStop() = 0;

    virtual std::vector<KafkaSourceDocument> doGetDocuments() = 0;

    const Options _options;
};

}  // namespace streams
