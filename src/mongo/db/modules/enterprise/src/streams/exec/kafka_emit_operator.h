#pragma once

#include "mongo/util/duration.h"
#include "streams/exec/sink_operator.h"

#include <rdkafka.h>
#include <rdkafkacpp.h>

namespace streams {
/**
 * The operator for targeting a Kafka topic.
 */
class KafkaEmitOperator : public SinkOperator {
public:
    struct Options {
        // List of bootstrap servers to specify in Kafka's bootstrap.servers configuration
        // parameter.
        std::string bootstrapServers;
        // Name of the topic to emit to.
        std::string topicName;
        // Auth related config options like "sasl.username".
        mongo::stdx::unordered_map<std::string, std::string> authConfig;
        // Flush timeout in milliseconds. Defaults to 10 minutes.
        // Note: we should keep this in sync with the max queue buffer setting,
        // which is currently 16MB.
        mongo::Milliseconds flushTimeout{mongo::Minutes(10)};
        // Partition to write to. If not specified, PARTITION_UA is supplied to librdkafka,
        // which will write to random partitions. Explicit partition is currently only
        // used in testing.
        boost::optional<int32_t> testOnlyPartition;
    };

    KafkaEmitOperator(Context* context, Options options);

    // Retrieve the options used for this instance.
    // Only used in testing.
    const Options& getOptions() const {
        return _options;
    }

    // This is called by doOnDataMsg() to write the documents to the sink.
    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;

    void doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        // This operator simply eats any control messages it receives.
    }

protected:
    std::string doGetName() const override {
        return "KafkaEmitOperator";
    }

    void doStop() final;

    // The librdkafka _producer.produce() call just puts messages in a background queue.
    // Here we flush those messages.
    void doFlush() final;

private:
    void processStreamDoc(const StreamDocument& streamDoc);

    // Creates an instance of RdKafka::Conf that can be used to create an instance of
    // RdKafka::Producer.
    std::unique_ptr<RdKafka::Conf> createKafkaConf();

    Options _options;
    std::unique_ptr<RdKafka::Conf> _conf{nullptr};
    std::unique_ptr<RdKafka::Producer> _producer{nullptr};
    // Default is to output to "any partition".
    int _outputPartition{RdKafka::Topic::PARTITION_UA};
};
}  // namespace streams
