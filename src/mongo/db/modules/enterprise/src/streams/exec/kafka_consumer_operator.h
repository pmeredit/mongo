#pragma once

#include <queue>
#include <rdkafkacpp.h>

#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/watermark_combiner.h"
#include "streams/exec/watermark_generator.h"

namespace streams {

class DeadLetterQueue;
class DocumentTimestampExtractor;
class EventDeserializer;
class KafkaPartitionConsumerBase;

/**
 * This is a source operator for a Kafka topic. It tails documents from a Kafka
 * topic and feeds those documents to the OperatorDag.
 */
class KafkaConsumerOperator : public SourceOperator {
public:
    struct PartitionOptions {
        // Partition of the topic to tail.
        int32_t partition{0};
        // Start offset in the partition to start tailing from.
        int64_t startOffset{RdKafka::Topic::OFFSET_BEGINNING};
        // May be nullptr. The watermarkGenerator to use, which may have an allowedLateness.
        std::unique_ptr<DelayedWatermarkGenerator> watermarkGenerator;
    };

    struct Options {
        // List of bootstrap servers to specify in Kafka's bootstrap.servers configuration
        // parameter.
        std::string bootstrapServers;
        // Name of the topic to tail.
        std::string topicName;
        std::vector<PartitionOptions> partitionOptions;
        // Dead letter queue to which documents that could not be processed are added.
        DeadLetterQueue* deadLetterQueue{nullptr};
        // EventDeserializer to use to deserialize Kafka messages to mongo::Documents.
        EventDeserializer* deserializer{nullptr};
        // May be nullptr. Used to extract event timestamp from a document.
        DocumentTimestampExtractor* timestampExtractor{nullptr};
        // The field name to use to store the event timestamp in the document.
        std::string timestampOutputFieldName = "_ts";
        // Maximum number of documents getDocuments() should return per call.
        int32_t maxNumDocsToReturn{500};
        // If true, test kafka partition consumers are used.
        bool isTest{false};
        // May be nullptr. Used to combine watermarks from multiple partitions.
        std::unique_ptr<WatermarkCombiner> watermarkCombiner;
    };

    KafkaConsumerOperator(Options options);

    // Retrieve the options used for this instance.
    // Only used in testing.
    const Options& getOptions() const {
        return _options;
    }

private:
    friend class KafkaConsumerOperatorTest;
    friend class WindowOperatorTest;

    // Encapsulates state for a Kafka partition consumer.
    struct ConsumerInfo {
        // Reads documents from this Kafka partition.
        std::unique_ptr<KafkaPartitionConsumerBase> consumer;
        // Generates watermarks for this Kafka partition.
        WatermarkGenerator* watermarkGenerator{nullptr};
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

    // Does the actual work of sourceLoop() and is called repeatedly by sourceLoop().
    // Returns the number of docs read from the partition consumers during this run.
    int32_t doRunOnce() override;

    // Processes the given KafkaSourceDocument and returns the corresponding StreamDocument.
    // Throw an exception if any error is encountered.
    boost::optional<StreamDocument> processSourceDocument(KafkaSourceDocument sourceDoc,
                                                          WatermarkGenerator* watermarkGenerator);

    Options _options;
    // KafkaPartitionConsumerBase instances, one for each partition.
    std::vector<ConsumerInfo> _consumers;
    StreamControlMsg _lastControlMsg;
};

}  // namespace streams
