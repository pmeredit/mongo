#pragma once

#include <fmt/format.h>
#include <queue>
#include <rdkafkacpp.h>

#include "mongo/stdx/unordered_map.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/watermark_combiner.h"
#include "streams/exec/watermark_generator.h"

namespace mongo {
class KafkaSourceCheckpointState;
};  // namespace mongo

namespace streams {

class DocumentTimestampExtractor;
class EventDeserializer;
class KafkaPartitionConsumerBase;
class OldCheckpointStorage;
struct Context;

/**
 * This is a source operator for a Kafka topic. It tails documents from a Kafka
 * topic and feeds those documents to the OperatorDag.
 */
class KafkaConsumerOperator : public SourceOperator {
public:
    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions)
            : SourceOperator::Options(std::move(baseOptions)) {}

        Options() = default;

        // List of bootstrap servers to specify in Kafka's bootstrap.servers configuration
        // parameter.
        std::string bootstrapServers;
        // Name of the topic to tail.
        std::string topicName;
        // Consumer group ID to use for the kafka consumer. If this is not set by the
        // user on creation, then an auto-generated one will be used which will look like:
        // `asp-{streamProcessorId}-consumer`. On checkpoint restoration, the consumer group
        // ID stored on the checkpoint will be used.
        std::string consumerGroupId;
        // The number of Kafka topic partitions.
        // When this is not provided, we fetch it from Kafka. Currently, this is only provided when
        // FakeKafkaPartitionConsumer is used.
        boost::optional<int64_t> testOnlyNumPartitions;
        // Start offset in each partition to start tailing from.
        int64_t startOffset{RdKafka::Topic::OFFSET_BEGINNING};
        // EventDeserializer to use to deserialize Kafka messages to mongo::Documents.
        EventDeserializer* deserializer{nullptr};
        // Maximum number of documents getDocuments() should return per call.
        int32_t maxNumDocsToReturn{kDataMsgMaxDocSize};
        // If true, test kafka partition consumers are used.
        bool isTest{false};
        // Auth related config options like "sasl.username".
        mongo::stdx::unordered_map<std::string, std::string> authConfig;
        // Partition idle timeout specified in the $source. A value of zero indicates that idleness
        // detection is disabled.
        mongo::stdx::chrono::milliseconds partitionIdleTimeoutMs{0};
        // Timeout used for Kafka api calls.
        mongo::stdx::chrono::milliseconds kafkaRequestTimeoutMs{10'000};
        // Sleep duration after Kafka api calls fail.
        mongo::stdx::chrono::milliseconds kafkaRequestFailureSleepDurationMs{1'000};
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

    // Commits the offset for the corresponding checkpoint ID to the kafka broker.
    void commitOffsets(CheckpointId checkpointId);

protected:
    // Merges stats from all the partition consumers.
    OperatorStats doGetStats() override;

private:
    friend class KafkaConsumerOperatorTest;
    friend class WindowOperatorTest;
    friend class CheckpointTestWorkload;
    friend class PlannerTest;

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
        // Partition idle timeout specified in the $source. A value of zero indicates that idleness
        // detection is disabled.
        mongo::stdx::chrono::milliseconds partitionIdleTimeoutMs{0};
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

    void processCheckpointMsg(const StreamControlMsg& controlMsg);

    std::string doGetName() const override {
        return "KafkaConsumerOperator";
    }

    // Initiates connection with the input source and initializes per-partition consumer instances.
    void doConnect() override;

    ConnectionStatus doGetConnectionStatus() override;

    // Does the actual work of sourceLoop() and is called repeatedly by sourceLoop().
    // Returns the number of docs read from the partition consumers during this run.
    int64_t doRunOnce() override;

    // Fetches topic partition count from Kafka using _metadataConsumer.
    void fetchNumPartitions();

    // Initializes the internal state from a checkpoint.
    void initFromCheckpoint();

    // Initializes the internal state from Options.
    void initFromOptions();

    // Initializes operator state using either initFromCheckpoint() or initFromOptions().
    void init();

    // Processes the given KafkaSourceDocument and returns the corresponding StreamDocument.
    // Throw an exception if any error is encountered.
    boost::optional<StreamDocument> processSourceDocument(KafkaSourceDocument sourceDoc,
                                                          WatermarkGenerator* watermarkGenerator);

    // Builds a DLQ message for the given KafkaSourceDocument.
    mongo::BSONObjBuilder toDeadLetterQueueMsg(KafkaSourceDocument sourceDoc);

    // Create a partition consumer. Used in constructor and initFromCheckpoint().
    ConsumerInfo createPartitionConsumer(int32_t partitionId, int64_t startOffset);

    // Creates a `KafkaConsumer` which is used as a proxy to commit offsets and fetch committed
    // offsets for the specified consumer group ID.
    std::unique_ptr<RdKafka::KafkaConsumer> createKafkaConsumer() const;

    // Gets the committed offsets for the consumer group ID set for this kafka consumer operator.
    // This must be called after the number of partitions has been fetched for the topic, so
    // `_numPartitions` must already be set when this is called. This returns a vector indexed by
    // the partition ID where the corresponding value is the committed offset for that partition ID.
    // If this consumer group ID does not exist or doesn't have any committed offsets, then this
    // will return an empty vector.
    std::vector<int64_t> getCommittedOffsets() const;

    Options _options;
    boost::optional<ConsumerInfo> _metadataConsumer;
    // The number of Kafka topic partitions.
    boost::optional<int64_t> _numPartitions;
    std::unique_ptr<WatermarkCombiner> _watermarkCombiner;
    // KafkaPartitionConsumerBase instances, one for each partition.
    std::vector<ConsumerInfo> _consumers;
    // Checkpoints that were triggered and saved but not yet committed yet. This occurs in
    // window fast-mode checkpointing, which holds onto incoming checkpoint messages until
    // the corresponding windows are closed. Checkpoints are always added to in chronological
    // order and they are always committed/popped in FIFO order.
    std::queue<std::pair<CheckpointId, mongo::KafkaSourceCheckpointState>> _uncommittedCheckpoints;
};

}  // namespace streams
