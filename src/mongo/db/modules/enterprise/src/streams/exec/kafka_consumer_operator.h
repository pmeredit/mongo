#pragma once

#include <queue>
#include <rdkafkacpp.h>

#include "mongo/platform/mutex.h"
#include "mongo/stdx/thread.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/watermark_combiner.h"
#include "streams/exec/watermark_generator.h"

namespace streams {

class DocumentTimestampExtractor;
class EventDeserializer;
class KafkaPartitionConsumerBase;

/**
 * This is a source operator for a Kafka topic. It continuously tails documents from a Kafka
 * topic and feeds those documents to the OperatorDag.
 */
class KafkaConsumerOperator : public Operator {
public:
    struct PartitionOptions {
        // Partition of the topic to tail.
        int32_t partition{0};
        // Start offset in the partition to start tailing from.
        int64_t startOffset{RdKafka::Topic::OFFSET_BEGINNING};
        // The delay in advancing the watermark.
        int64_t watermarkGeneratorAllowedLatenessMs{0};
    };

    struct Options {
        // List of bootstrap servers to specify in Kafka's bootstrap.servers configuration
        // parameter.
        std::string bootstrapServers;
        // Name of the topic to tail.
        std::string topicName;
        std::vector<PartitionOptions> partitionOptions;
        // Sleep duration when all partitions are idle.
        int32_t sourceIdleSleepDurationMs{2000};
        // EventDeserializer to use to deserialize Kafka messages to mongo::Documents.
        EventDeserializer* deserializer{nullptr};
        // Used to extract event timestamp from a document.
        DocumentTimestampExtractor* timestampExtractor{nullptr};
        // The field name to use to store the event timestamp in the document.
        mongo::StringData timestampOutputFieldName;
        // Maximum number of documents getDocuments() should return per call.
        int32_t maxNumDocsToReturn{500};
    };

    KafkaConsumerOperator(Options options);

private:
    friend class KafkaConsumerOperatorTest;

    // Encapsulates state for a Kafka partition consumer.
    struct ConsumerInfo {
        // Reads documents from this Kafka partition.
        std::unique_ptr<KafkaPartitionConsumerBase> consumer;
        // Generates watermarks for this Kafka partition.
        std::unique_ptr<WatermarkGenerator> watermarkGenerator;
    };

    void doStart() override;
    void doStop() override;
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override {
        MONGO_UNREACHABLE;
    }
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        MONGO_UNREACHABLE;
    }

    std::string doGetName() const override {
        return "KafkaConsumerOperator";
    }

    // _sourceThread uses this to continuously tail documents from the Kafka topic
    // and feed those documents to the OperatorDag.
    void sourceLoop();

    // Does the actual work of sourceLoop() and is called repeatedly by sourceLoop().
    // Returns the number of docs read from the partition consumers during this run.
    int32_t runOnce();

    // Processes the given KafkaSourceDocument and returns the corresponding StreamDocument.
    // Throw an exception if any error is encountered.
    StreamDocument processSourceDocument(KafkaSourceDocument sourceDoc);

    Options _options;
    // Combines watermarks of all Kafka partitions to generate a watermark for this operator.
    std::unique_ptr<WatermarkCombiner> _watermarkCombiner;
    // KafkaPartitionConsumerBase instances, one for each partition.
    std::vector<ConsumerInfo> _consumers;
    mongo::stdx::thread _sourceThread;
    StreamControlMsg _lastControlMsg;
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("KafkaConsumerOperator::mutex");
    bool _shutdown{false};
};

}  // namespace streams
