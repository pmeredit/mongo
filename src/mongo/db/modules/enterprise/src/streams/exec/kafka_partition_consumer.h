#pragma once

#include <exception>
#include <queue>
#include <rdkafkacpp.h>
#include <string>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/basic.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/kafka_event_callback.h"
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/message.h"

namespace streams {

class EventDeserializer;
struct Context;

/**
 * This class continuously tails documents from a partition of a Kafka topic and returns the
 * documents to the caller.
 * This class is not thread-safe.
 */
class KafkaPartitionConsumer : public KafkaPartitionConsumerBase {
public:
    KafkaPartitionConsumer(Context* context, Options options);

    ~KafkaPartitionConsumer() override;

    // Creates an instance of RdKafka::Conf that can be used to create an instance of
    // RdKafka::Consumer.
    std::unique_ptr<RdKafka::Conf> createKafkaConf();

    int32_t partition() const {
        return _options.partition;
    }

private:
    friend class ConsumeCbImpl;  // Needed to be able to call onMessage() and onError().

    // Encapsulates a batch of documents read from the Kafka partition.
    // Caller is responsible for acquiring DocBatch.mutex before calling any methods of this struct.
    struct DocBatch {
        struct DocVec {
            DocVec(size_t capacity) {
                docs.reserve(capacity);
            }

            // Appends the given doc to docs.
            void pushDoc(KafkaSourceDocument doc);

            int64_t size() const {
                return docs.size();
            }

            int64_t getByteSize() const {
                return byteSize;
            }

            int64_t capacity() const {
                return docs.capacity();
            }

            std::vector<KafkaSourceDocument> docs;
            // Tracks the total number of bytes in docs.
            int32_t byteSize{0};
        };

        int64_t size() const;

        int64_t getByteSize() const;

        bool empty() const;

        // Appends an empty DocVec docVecs. Also, reserves the specified capacity in the new DocVec.
        void emplaceDocVec(size_t capacity);

        // Appends the document to the last DocVec in docVecs.
        // Caller is responsible for ensuring that the last DocVec has not already exceeded the
        // size limit of Options.maxNumDocsToReturn.
        void pushDocToLastDocVec(KafkaSourceDocument doc);

        // Appends the given DocVec docVecs.
        void pushDocVec(DocVec docVec);

        // Pops the first DocVec from docVecs and returns it.
        DocVec popDocVec();

        // Guards all members of this struct.
        mutable mongo::Mutex mutex = MONGO_MAKE_LATCH("KafkaPartitionConsumer::DocBatch::mutex");
        // Tracks all the documents added to this batch.
        std::queue<DocVec> docVecs;
        // Tracks the total number of documents in docVecs.
        int64_t numDocs{0};
        // Tracks the total number of bytes in docs.
        int64_t byteSize{0};
        // Tracks the total number of documents returned to the caller via popDocVec().
        int64_t numDocsReturned{0};
        // Tracks an error status that needs to be returned to the caller.
        SPStatus status;
        // Whether _consumerThread should shut down. This is triggered when stop() is called or
        // an error is encountered. Currently this is only set in _finalizedDocBatch.
        bool shutdown{false};
    };

    // Initializes necessary internal state like _consumer and _topic.
    // Throws an exception if any error is encountered during the initialization.
    void doInit() override;

    // Starts _consumerThread which continuously calls consume_callback() to prefetch
    // the specified number of documents from the Kafka partition.
    void doStart() override;

    // Stops _consumerThread. If start() was called previously, stop() must also be called
    // before the destructor is invoked.
    void doStop() override;

    // Whether the consumer is connected to the source Kafka cluster.
    ConnectionStatus doGetConnectionStatus() const override;

    // Returns _startOffset.
    boost::optional<int64_t> doGetStartOffset() const override;

    // Returns _numPartitions.
    boost::optional<int64_t> doGetNumPartitions() const override;

    // Returns the next batch of documents tailed from the partition, if any available.
    // Throws exception if any exception was encountered while tailing Kafka.
    std::vector<KafkaSourceDocument> doGetDocuments() override;

    // Returns the stats for this partition.
    OperatorStats doGetStats() override;

    // Calls query_watermark_offsets() to find the low and high offsets for the partition
    // and returns the appropriate start offset to use for this partition.
    // Returns boost::none if there was an error in reading the offsets.
    int64_t queryWatermarkOffsets();

    // Tries connection to the source Kafka cluster and retrieve start offset.
    void connectToSource();

    // _consumerThread uses this to continuously tail documents from the Kafka partition.
    void fetchLoop();

    // Called by RdKafka::ConsumeCb implementation to forward the messages received from librdkafka.
    // It deserializes the message into a mongo::Document and adds the document to _activeDocBatch.
    void onMessage(RdKafka::Message& msg);

    // Registers the given error and initiates the shutdown of _consumerThread.
    void onError(SPStatus status);

    // Called when there is an error connecting to Kafka.
    void onConnectionError(SPStatus status);

    // Processes the given RdKafka::Message that contains a payload and returns the corresponding
    // KafkaSourceDocument.
    KafkaSourceDocument processMessagePayload(RdKafka::Message& message);

    // Adds the given document to _activeDocBatch. If any DocVecs in _activeDocBatch
    // reached their maximum size of Options.maxNumDocsToReturn, it also moves them to
    // _finalizedDocBatch.
    void pushDocToActiveDocBatch(KafkaSourceDocument doc);

    std::unique_ptr<KafkaEventCallback> _eventCallback;
    std::unique_ptr<RdKafka::Conf> _conf{nullptr};
    std::unique_ptr<RdKafka::Consumer> _consumer{nullptr};
    std::unique_ptr<RdKafka::Topic> _topic{nullptr};

    mongo::stdx::thread _consumerThread;
    // Whenever the mutexes in the following 2 DocBatches need to be acquired together,
    // we always acquire _finalizedDocBatch.mutex before acquiring _activeDocBatch.mutex
    // to avoid a deadlock.
    DocBatch _finalizedDocBatch;
    DocBatch _activeDocBatch;
    // condition_variable used by _consumerThread. Use _finalizedDocBatch.mutex with this
    // condition_variable.
    mongo::stdx::condition_variable _consumerThreadWakeUpCond;

    // Guards following member variables.
    // Note that this mutex and DocBatch::mutex are never acquired together.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("KafkaPartitionConsumer::mutex");
    // Whether this consumer is currently connected to the source Kafka cluster.
    // The initial offset used to start tailing the Kafka partition.
    boost::optional<int64_t> _startOffset;
    // The number of Kafka topic partitions.
    boost::optional<int64_t> _numPartitions;
    // Connection status of the partition consumer. The background thread updates
    // this as it connects (or errors).
    ConnectionStatus _connectionStatus;
    // Context of the streamProcessor used for logging purposes.
    Context* _context{nullptr};
    // Memory usage handle to track queued prefetch buffer. This only tracks completed batches that
    // are added to `_finalizedDocBatch`. This should only be updated under the `_finalizedDocBatch`
    // mutex.
    mongo::MemoryUsageHandle _memoryUsageHandle;
};

}  // namespace streams
