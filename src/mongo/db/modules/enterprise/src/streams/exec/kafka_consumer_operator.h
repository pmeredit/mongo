/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <fmt/format.h>
#include <queue>
#include <rdkafkacpp.h>

#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/unflushed_state_container.h"
#include "streams/exec/watermark_combiner.h"
#include "streams/exec/watermark_generator.h"

namespace mongo {
class KafkaSourceCheckpointState;
};  // namespace mongo

namespace streams {

class DocumentTimestampExtractor;
class EventDeserializer;
class KafkaPartitionConsumerBase;
struct Context;

/**
 * This is a source operator for Kafka topic(s). It tails documents from Kafka
 * topic(s) and feeds those documents to the OperatorDag.
 */
class KafkaConsumerOperator : public SourceOperator {
public:
    // Helper type used in some internal methods to represent a topic and partition
    struct TopicPartition {
        std::string topic;
        int32_t partitionId;

        TopicPartition(std::string topicName, int32_t partition);
        bool operator==(const TopicPartition& other) const = default;
        mongo::BSONObj toBSON() const;
    };

    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions)
            : SourceOperator::Options(std::move(baseOptions)) {}

        Options() = default;

        // List of bootstrap servers to specify in Kafka's bootstrap.servers configuration
        // parameter.
        std::string bootstrapServers;
        // Name of the topic(s) to tail.
        std::vector<std::string> topicNames;
        // Consumer group ID to use for the kafka consumer. If this is not set by the
        // user on creation, then an auto-generated one will be used (when the stream is not
        // ephemeral) which will look like: `asp-{streamProcessorId}-consumer` . On checkpoint
        // restoration, the consumer group ID stored on the checkpoint will be used.
        std::string consumerGroupId;
        // This represents the partitions for each topic. When it is not provided, we fetch it from
        // the kafka cluster. Currently, this is only provided when FakeKafkaPartitionConsumer is
        // used (i.e. in unittests).
        std::vector<TopicPartition> testOnlyTopicPartitions;
        // Start offset in each partition to start tailing from.
        int64_t startOffset{RdKafka::Topic::OFFSET_BEGINNING};
        // EventDeserializer to use to deserialize Kafka messages to mongo::Documents.
        EventDeserializer* deserializer{nullptr};
        // Maximum number of documents getDocuments() should return per call.
        int32_t maxNumDocsToReturn{500};
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
        // GWProxy endpoint hostname or IP address.
        boost::optional<std::string> gwproxyEndpoint;
        // GWProxy endpoint symmetric encryption key.
        boost::optional<std::string> gwproxyKey;
        // The data type used to deserialize Kafka key.
        mongo::KafkaKeyFormatEnum keyFormat{mongo::KafkaKeyFormatEnum::BinData};
        // How to handle error during Kafka key deserialization.
        mongo::KafkaSourceKeyFormatErrorEnum keyFormatError{
            mongo::KafkaSourceKeyFormatErrorEnum::Dlq};
        // Whether to commit offsets on reads or on checkpoints. true indicates that offsets will be
        // commited on reads.
        bool enableAutoCommit{false};
    };

    KafkaConsumerOperator(Context* context, Options options);

    // Retrieve the options used for this instance.
    // Only used in testing.
    const Options& getOptions() const override {
        return _options;
    }

    // Only usable if _consumers is a FakeKafkaPartitionConsumer.
    // Inserts some docs into the FakeKafkaPartitionConsumer.
    void testOnlyInsertDocuments(std::vector<mongo::BSONObj> docs);

    // Returns a snapshot of the current state of each partition for this kafka source. This is not
    // thread-safe so it cannot be called from outside the executor thread, e.g. this cannot be
    // called in parallel with `runOnce()`.
    std::vector<KafkaConsumerPartitionState> getPartitionStates() const;

protected:
    // Merges stats from all the partition consumers.
    OperatorStats doGetStats() override;

    void registerMetrics(MetricManager* metricManager) override;

private:
    friend class KafkaConsumerOperatorTest;
    friend class WindowOperatorTest;
    friend class CheckpointTestWorkload;
    friend class PlannerTest;
    friend class KafkaKeyDeserializationTest;

    // Encapsulates state for a Kafka partition consumer.
    struct ConsumerInfo {
        // Reads documents from this Kafka partition.
        std::unique_ptr<KafkaPartitionConsumerBase> consumer;
        // Generates watermarks for this Kafka partition.
        std::unique_ptr<WatermarkGenerator> watermarkGenerator;
        // The topic that this consumer will subscribe to.
        std::string topic;
        // The partition in the topic that the consumer will subscribe to.
        int32_t partition{0};
        // Max received offset. This is updated in runOnce as documents are flushed
        // from consumers. If a document has not been consumed yet from this partition
        // and the partition starting offset is the beginning of the partition, then
        // this will be set to `-1`.
        boost::optional<int64_t> maxOffset;
        // Last committed offset to the kafka broker and the checkpoint for the corresponding
        // consumer group ID.
        int64_t checkpointOffset{0};
        // Partition idle timeout specified in the $source. A value of zero indicates that idleness
        // detection is disabled.
        mongo::stdx::chrono::milliseconds partitionIdleTimeoutMs{0};
        // Tracks the wall clock time of the last read event. Used for detecting idleness in a
        // partition
        mongo::stdx::chrono::time_point<mongo::stdx::chrono::steady_clock> lastEventReadTimestamp;
    };

    // This class encapsulates the initial connection logic for this operator.
    class Connector {
    public:
        struct Options {
            std::vector<std::string> topicNames;
            mongo::stdx::chrono::milliseconds kafkaRequestTimeoutMs;
            // Sleep duration after Kafka api calls fail.
            mongo::stdx::chrono::milliseconds kafkaRequestFailureSleepDurationMs{1'000};
            std::string bootstrapServers;
            std::string consumerGroupId;
            mongo::stdx::unordered_map<std::string, std::string> authConfig;
            boost::optional<std::string> gwproxyEndpoint;
            boost::optional<std::string> gwproxyKey;
        };

        Connector(Context* context, Options options);

        ~Connector();

        // Starts the background thread.
        void start();

        // Stops the background thread.
        void stop();

        // Returns the current connection status.
        ConnectionStatus getConnectionStatus();

        // Returns partitions of the topic that the source is subscribed to.
        std::vector<TopicPartition> getTopicPartitions();

    private:
        void setConnectionStatus(ConnectionStatus status);

        // Creates the _consumer from the configured options.
        void createKafkaConsumer();

        // Fetches the partition info for the subscribed topic.
        // It will uassert on any errors and the call site will append error
        // details retrieved via the configured KafkaEventCallback.
        // On success, it will set state to Connected.
        void retrieveTopicPartitions();

        // Retrieves error details via configured event callback.
        void onConnectionError(SPStatus status);

        Context* _context{nullptr};
        Options _options;
        // Background thread used to establish connection with Kafka.
        mongo::stdx::thread _connectionThread;
        // Protects the members below.
        mutable mongo::stdx::mutex _mutex;
        bool _shutdown{false};
        // Tracks the current ConnectionStatus.
        ConnectionStatus _connectionStatus;
        std::vector<TopicPartition> _topicPartitions;
        // Used to retrieve error details
        std::unique_ptr<KafkaEventCallback> _eventCallback;
        // Support for GWProxy authentication callbacks to enable VPC peering sessions.
        std::unique_ptr<RdKafka::ConnectCb> _connectCbImpl;
        std::unique_ptr<RdKafka::ResolveCb> _resolveCbImpl;
        // KafkaConsumer instance used to determine the topic/partition map for the topics we
        // will be subscribing to
        std::unique_ptr<RdKafka::KafkaConsumer> _consumer;
    };

    struct TopicPartitionCmp {
        bool operator()(const TopicPartition& lhs, const TopicPartition& rhs) const {
            if (lhs.topic == rhs.topic) {
                return lhs.partitionId < rhs.partitionId;
            }
            return lhs.topic < rhs.topic;
        }
    };
    using TopicPartitionOffsetMap = std::map<TopicPartition, int64_t, TopicPartitionCmp>;

    void doStart() override;
    void doStop() override;
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override {
        MONGO_UNREACHABLE;
    }
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;
    mongo::BSONObj doOnCheckpointFlush(CheckpointId checkpointId) override;
    boost::optional<mongo::BSONObj> doGetRestoredState() override;
    boost::optional<mongo::BSONObj> doGetLastCommittedState() override;

    void processCheckpointMsg(const StreamControlMsg& controlMsg);

    std::string doGetName() const override {
        return "KafkaConsumerOperator";
    }

    ConnectionStatus doGetConnectionStatus() override;

    // Does the actual work of sourceLoop() and is called repeatedly by sourceLoop().
    // Returns the number of docs read from the partition consumers during this run.
    int64_t doRunOnce() override;

    // Initializes the internal state from a checkpoint.
    void initFromCheckpoint();

    // Initializes the internal state from Options.
    void initFromOptions();

    // Initializes operator state using either initFromCheckpoint() or initFromOptions().
    void init();

    // The background task for the group consumer.
    void groupConsumerBackgroundLoop();

    // Processes the given KafkaSourceDocument and returns the corresponding StreamDocument.
    // Throw an exception if any error is encountered.
    boost::optional<StreamDocument> processSourceDocument(KafkaSourceDocument sourceDoc,
                                                          WatermarkGenerator* watermarkGenerator);

    // Builds a DLQ message for the given KafkaSourceDocument.
    mongo::BSONObjBuilder toDeadLetterQueueMsg(KafkaSourceDocument sourceDoc);

    // Helper methods to create a partition consumer.
    std::unique_ptr<KafkaPartitionConsumerBase> createKafkaPartitionConsumer(
        std::string topicName,
        int32_t partition,
        int64_t startOffset,
        boost::optional<int64_t> rdkafkaQueuedMaxMessagesKBytes);
    ConsumerInfo createPartitionConsumer(std::string topicName,
                                         int32_t partitionId,
                                         int64_t startOffset,
                                         boost::optional<int64_t> rdkafkaQueuedMaxMessagesKBytes);

    // Creates a `KafkaConsumer` which is used as a proxy to commit offsets and fetch committed
    // offsets for the specified consumer group ID.
    std::unique_ptr<RdKafka::KafkaConsumer> createKafkaConsumer();

    // Gets the committed offsets for the consumer group ID set for this kafka consumer operator.
    // This must be called after the topic partition ids have been fetched for the topics, so
    // `_topicPartitions` must be non-empty when this is called.
    // If this consumer group ID does not exist or doesn't have any committed offsets, then this
    // will return an empty map.
    TopicPartitionOffsetMap getCommittedOffsets() const;

    // Deserialize the Kafka key according to the specified key format. If the deserialization
    // fails, the key will be returned as BinData.
    static std::variant<std::vector<std::uint8_t>,
                        std::string,
                        mongo::BSONObj,
                        std::int32_t,
                        std::int64_t,
                        double>
    deserializeKafkaKey(std::vector<std::uint8_t> key, mongo::KafkaKeyFormatEnum keyFormat);

    const std::vector<TopicPartition>& getTopicPartitions() const {
        return _topicPartitions;
    }

    // Returns the index of a topic-partition that is used by the watermark combiner for
    // this topic-partition.
    boost::optional<int32_t> getPartitionIdx(const std::string& topicName, int32_t partitionId);

    Options _options;
    boost::optional<mongo::KafkaSourceCheckpointState> _restoredCheckpointState;
    std::unique_ptr<Connector> _connector;
    std::vector<TopicPartition> _topicPartitions;
    std::unique_ptr<WatermarkCombiner> _watermarkCombiner;
    // KafkaPartitionConsumerBase instances, one for each partition.
    std::vector<ConsumerInfo> _consumers;
    int32_t _testOnlyDataMsgMaxDocSize{std::numeric_limits<int32_t>::max()};
    // Checkpoint state that has not yet been flushed to remote storage.
    UnflushedStateContainer _unflushedStateContainer;

    // Kafka $source state in the last committed checkpoint.
    boost::optional<mongo::KafkaSourceCheckpointState> _lastCommittedCheckpointState;

    // Metrics that track the number of docs and bytes prefetched.
    std::shared_ptr<IntGauge> _queueSizeGauge;
    std::shared_ptr<IntGauge> _queueByteSizeGauge;

    // The _groupConsumer instance is used to retrieve and commit offsets to a Kafka consumer group.
    // We don't actually use this instance for reading messages.
    std::unique_ptr<RdKafka::KafkaConsumer> _groupConsumer;

    // The _groupConsumer background thread, used to ocassionally call consume which rdkafka
    // requires.
    mongo::stdx::thread _groupConsumerThread;
    // This mutex protects the variables below.
    mutable mongo::stdx::mutex _groupConsumerMutex;
    mongo::stdx::condition_variable _groupConsumerThreadCond;
    bool _groupConsumerThreadShutdown{false};

    // Support for GWProxy authentication callbacks to enable VPC peering sessions.
    std::unique_ptr<RdKafka::ConnectCb> _connectCbImpl;
    std::unique_ptr<RdKafka::ResolveCb> _resolveCbImpl;

    // Used in doRunOnce to handle committing offsets to the consumer group. Only used when
    // config.enable_auto_commit is true.
    std::vector<RdKafka::TopicPartition*> _partitionOffsets;
    std::vector<std::unique_ptr<RdKafka::TopicPartition>> _partitionOffsetsHolder;
};

}  // namespace streams
