#pragma once

#include <queue>

#include "mongo/platform/mutex.h"
#include "mongo/stdx/thread.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"

namespace streams {

class EventDeserializer;
class KafkaPartitionConsumer;

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
        int64_t startOffset{0};
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
        // Maximum number of documents getDocuments() should return per call.
        int32_t maxNumDocsToReturn{500};
    };

    KafkaConsumerOperator(Options options);

private:
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
    // Returns true if any docs were read from the partition consumers during this run.
    bool runOnce();

    Options _options;
    // KafkaPartitionConsumer instances, one for each partition.
    std::vector<std::unique_ptr<KafkaPartitionConsumer>> _consumers;
    mongo::stdx::thread _sourceThread;
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("KafkaConsumerOperator::mutex");
    bool _shutdown{false};
};

}  // namespace streams
