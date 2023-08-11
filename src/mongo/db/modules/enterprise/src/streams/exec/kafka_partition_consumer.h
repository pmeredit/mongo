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
#include "streams/exec/kafka_partition_consumer_base.h"
#include "streams/exec/message.h"

namespace streams {

class EventDeserializer;

/**
 * This class continuously tails documents from a partition of a Kafka topic and returns the
 * documents to the caller.
 * This class is not thread-safe.
 */
class KafkaPartitionConsumer : public KafkaPartitionConsumerBase {
public:
    KafkaPartitionConsumer(Options options) : KafkaPartitionConsumerBase(std::move(options)) {}

    ~KafkaPartitionConsumer();

    int32_t partition() const {
        return _options.partition;
    }

private:
    friend class ConsumeCbImpl;  // Needed to be able to call onMessage() and onError().

    // Encapsulates a batch of documents read from the Kafka partition.
    // Caller is responsible for acquiring DocBatch.mutex before calling any methods of this struct.
    struct DocBatch {
        using DocVec = std::vector<KafkaSourceDocument>;

        int32_t size() const;

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
        int32_t numDocs{0};
        // Tracks the total number of documents returned to the caller via popDocVec().
        int64_t numDocsReturned{0};
        // Tracks an exception that needs to be returned to the caller.
        std::exception_ptr exception;
        // Whether consumerThread should shut down. This is triggered when stop() is called or
        // an error is encountered. Currently this is only set in _finalizedDocBatch.
        bool shutdown{false};
    };

    // Initializes necessary internal state like _consumer and _topic.
    // Throws an exception if any error is encountered during the initialization.
    void doInit() override;

    // Starts _consumerThread which continuously calls consume_callback() to prefetch
    // the specified number of documents from the Kafka partition.
    int64_t doStart() override;

    // Stops _consumerThread. If start() was called previously, stop() must also be called
    // before the destructor is invoked.
    void doStop() override;

    // Returns the next batch of documents tailed from the partition, if any available.
    // Throws exception if any exception was encountered while tailing Kafka.
    std::vector<KafkaSourceDocument> doGetDocuments() override;

    // Creates an instance of RdKafka::Conf that can be used to create an instance of
    // RdKafka::Consumer.
    std::unique_ptr<RdKafka::Conf> createKafkaConf();

    // _consumerThread uses this to continuously tail documents from the Kafka partition.
    void fetchLoop();

    // Deserializes the message received from Kafka into a mongo::Document and adds the
    // document to _activeDocBatch.
    void onMessage(const RdKafka::Message& msg);
    void onError(std::exception_ptr exception);

    // Processes the given RdKafka::Message that contains a payload and returns the corresponding
    // KafkaSourceDocument.
    KafkaSourceDocument processMessagePayload(const RdKafka::Message& message);

    // Adds the given document to _activeDocBatch. If any DocVecs in _activeDocBatch
    // reached their maximum size of Options.maxNumDocsToReturn, it also moves them to
    // _finalizedDocBatch.
    void pushDocToActiveDocBatch(KafkaSourceDocument doc);

    size_t getMaxDocVecSize() const {
        return size_t(_options.maxNumDocsToReturn);
    }

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
};

}  // namespace streams
