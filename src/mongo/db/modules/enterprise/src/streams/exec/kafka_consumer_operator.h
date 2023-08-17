#pragma once

#include <queue>
#include <rdkafkacpp.h>

#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/watermark_combiner.h"
#include "streams/exec/watermark_generator.h"

namespace streams {

class DocumentTimestampExtractor;
class EventDeserializer;
class KafkaPartitionConsumerBase;
class CheckpointStorage;
struct Context;

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
    };

    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions)
            : SourceOperator::Options(std::move(baseOptions)) {}

        Options() = default;

        // List of bootstrap servers to specify in Kafka's bootstrap.servers configuration
        // parameter.
        std::string bootstrapServers;
        // Name of the topic to tail.
        std::string topicName;
        std::vector<PartitionOptions> partitionOptions;
        // EventDeserializer to use to deserialize Kafka messages to mongo::Documents.
        EventDeserializer* deserializer{nullptr};
        // Maximum number of documents getDocuments() should return per call.
        int32_t maxNumDocsToReturn{500};
        // If true, test kafka partition consumers are used.
        bool isTest{false};
        // Auth related config options like "sasl.username".
        mongo::stdx::unordered_map<std::string, std::string> authConfig;
        // Idleness timeout specified in the $source. A value of zero indicates that idleness
        // detection is disabled.
        mongo::stdx::chrono::milliseconds idlenessTimeoutMs{0};
    };

    KafkaConsumerOperator(Context* context, Options options);

    // Retrieve the options used for this instance.
    // Only used in testing.
    const Options& getOptions() const {
        return _options;
    }

    // Only usable if _consumers is a FakeKafkaPartitionConsumer.
    // Inserts some docs into the FakeKafkaPartitionConsumer.
    void testOnlyInsertDocuments(std::vector<mongo::BSONObj> docs);

private:
    friend class KafkaConsumerOperatorTest;
    friend class WindowOperatorTest;
    friend class CheckpointTestWorkload;
    friend class ParserTest;

    // Encapsulates state for a Kafka partition consumer.
    struct ConsumerInfo {
        // Reads documents from this Kafka partition.
        std::unique_ptr<KafkaPartitionConsumerBase> consumer;
        // Generates watermarks for this Kafka partition.
        std::unique_ptr<WatermarkGenerator> watermarkGenerator;
        // The partition of the consumer.
        int32_t partition{0};
        // Max received offset. This is updated in runOnce as documents are flushed
        // from consumers.
        boost::optional<int64_t> maxOffset;
        // Idleness timeout specified in the $source. A value of zero indicates that idleness
        // detection is disabled.
        mongo::stdx::chrono::milliseconds idlenessTimeoutMs{0};
        // Tracks the wall clock time of the last read event. Used for detecting idleness in a
        // partition
        mongo::stdx::chrono::time_point<mongo::stdx::chrono::steady_clock> lastEventReadTimestamp;
    };

    void doStart() override;
    void doStop() override;
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override {
        MONGO_UNREACHABLE;
    }
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;
    void doRestoreFromCheckpoint(CheckpointId checkpointId) override;

    void processCheckpointMsg(const StreamControlMsg& controlMsg);

    std::string doGetName() const override {
        return "KafkaConsumerOperator";
    }

    // Does the actual work of sourceLoop() and is called repeatedly by sourceLoop().
    // Returns the number of docs read from the partition consumers during this run.
    int64_t doRunOnce() override;

    bool doIsConnected() override;

    // Processes the given KafkaSourceDocument and returns the corresponding StreamDocument.
    // Throw an exception if any error is encountered.
    boost::optional<StreamDocument> processSourceDocument(KafkaSourceDocument sourceDoc,
                                                          WatermarkGenerator* watermarkGenerator);

    // Builds a DLQ message for the given KafkaSourceDocument.
    mongo::BSONObjBuilder toDeadLetterQueueMsg(KafkaSourceDocument sourceDoc);

    // Create a partition consumer. Used in constructor and doRestoreFromCheckpoint.
    ConsumerInfo createPartitionConsumer(int32_t partitionId, int64_t startOffset);

    Options _options;
    // KafkaPartitionConsumerBase instances, one for each partition.
    std::vector<ConsumerInfo> _consumers;
    StreamControlMsg _lastControlMsg;
    std::unique_ptr<WatermarkCombiner> _watermarkCombiner;
    bool _started{false};
};

}  // namespace streams
