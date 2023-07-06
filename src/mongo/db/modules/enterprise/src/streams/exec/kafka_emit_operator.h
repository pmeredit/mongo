#pragma once

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
        // TODO SERVER-78645: Consider adding more configuration options.
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

    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        // This operator simply eats any control messages it receives.
    }

protected:
    std::string doGetName() const override {
        return "KafkaEmitOperator";
    }

private:
    void processStreamDoc(const StreamDocument& streamDoc);

    // Creates an instance of RdKafka::Conf that can be used to create an instance of
    // RdKafka::Producer.
    // TODO SERVER-78645: Determine if this can be deduplicated with
    // 'KafkaPartitionConsumer::createKafkaConf'.
    std::unique_ptr<RdKafka::Conf> createKafkaConf();

    Options _options;
    std::unique_ptr<RdKafka::Conf> _conf{nullptr};
    std::unique_ptr<RdKafka::Producer> _producer{nullptr};

    // TODO SERVER-78645: Consider introducing a separate thread responsible for flushing documents
    // to the output kafka topic.
};
}  // namespace streams
