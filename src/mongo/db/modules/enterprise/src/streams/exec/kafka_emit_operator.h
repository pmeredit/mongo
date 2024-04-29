#pragma once

#include <boost/intrusive_ptr.hpp>

#include "mongo/bson/oid.h"
#include "mongo/db/pipeline/name_expression.h"
#include "mongo/util/duration.h"
#include "mongo/util/string_map.h"
#include "streams/exec/kafka_connect_auth_callback.h"
#include "streams/exec/kafka_event_callback.h"
#include "streams/exec/kafka_resolve_callback.h"
#include "streams/exec/sink_operator.h"
#include "streams/exec/stages_gen.h"

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
        mongo::NameExpression topicName;
        // Auth related config options like "sasl.username".
        mongo::stdx::unordered_map<std::string, std::string> authConfig;
        // Flush timeout in milliseconds. Defaults to 1 minute.
        // Note: we should keep this in sync with the max queue buffer setting,
        // which is currently 16MB.
        mongo::Milliseconds flushTimeout{mongo::Minutes(1)};
        // Partition to write to. If not specified, PARTITION_UA is supplied to librdkafka,
        // which will write to random partitions. Explicit partition is currently only
        // used in testing.
        boost::optional<int32_t> testOnlyPartition;
        // Timeout we use when querying the metadata in doStart to validate the connection.
        mongo::Milliseconds metadataQueryTimeout{mongo::Seconds(10)};
        // The expression that evaluates to the key of the Kafka message.
        boost::intrusive_ptr<mongo::Expression> key{nullptr};
        // The expected data type used to serialize the key of the Kafka message.
        mongo::KafkaKeyFormatEnum keyFormat{mongo::KafkaKeyFormatEnum::BinData};
        // The expression that evaluates to the headers of the Kafka message.
        boost::intrusive_ptr<mongo::Expression> headers{nullptr};
        // GWProxy endpoint hostname or IP address.
        boost::optional<std::string> gwproxyEndpoint;
        // GWProxy symmetric key.
        boost::optional<std::string> gwproxyKey;
        // Json String Format either relaxedJson or canonicalJson.
        mongo::JsonStringFormat jsonStringFormat{mongo::JsonStringFormat::ExtendedRelaxedV2_0_0};
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

    void doStart() override;
    void doStop() override;
    ConnectionStatus doGetConnectionStatus() override;

    // The librdkafka _producer.produce() call just puts messages in a background queue.
    // Here we flush those messages.
    void doFlush() override;

private:
    // This class encapsulates the initial connection logic for this operator.
    class Connector {
    public:
        struct Options {
            boost::optional<std::string> topicName;
            RdKafka::Producer* producer{nullptr};
            // Timeout for the metadata query.
            mongo::Milliseconds metadataQueryTimeout{mongo::Seconds(10)};
            // Event callback.
            KafkaEventCallback* kafkaEventCallback{nullptr};
        };

        Connector(Options options);

        ~Connector();

        // Starts the background thread.
        void start();

        // Stops the background thread.
        void stop();

        // Returns the current connection status.
        ConnectionStatus getConnectionStatus();

    private:
        void setConnectionStatus(ConnectionStatus status);

        // Runs the connection logic until a success or error is encountered.
        void testConnection();

        Options _options;
        // Background thread used to establish connection with Kafka.
        mongo::stdx::thread _connectionThread;
        // Protects the members below.
        mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("Connector::mutex");
        // Tracks the current ConnectionStatus.
        ConnectionStatus _connectionStatus;
    };

    void processStreamDoc(const StreamDocument& streamDoc);

    RdKafka::Headers* createKafkaHeaders(const StreamDocument& streamDoc, std::string topicName);

    mongo::Value createKafkaKey(const StreamDocument& streamDoc);

    // Creates an instance of RdKafka::Conf that can be used to create an instance of
    // RdKafka::Producer.
    std::unique_ptr<RdKafka::Conf> createKafkaConf();

    Options _options;
    // Used to print librdkafka logs.
    std::unique_ptr<KafkaEventCallback> _eventCbImpl;
    std::unique_ptr<RdKafka::Conf> _conf{nullptr};
    std::unique_ptr<RdKafka::Producer> _producer{nullptr};
    std::unique_ptr<Connector> _connector;
    // Hold topic objects.
    mongo::StringMap<std::unique_ptr<RdKafka::Topic>> _topicCache;
    // Default is to output to "any partition".
    int _outputPartition{RdKafka::Topic::PARTITION_UA};

    // To evaluate the dynamic topic name.
    boost::intrusive_ptr<mongo::ExpressionContext> _expCtx{nullptr};

    // The ConnectionStatus of the $emit operator.
    ConnectionStatus _connectionStatus;

    // Support for GWProxy authentication callbacks to enable VPC peering sessions.
    std::unique_ptr<RdKafka::ResolveCb> _resolveCbImpl;
    std::unique_ptr<RdKafka::ConnectCb> _connectCbImpl;
};
}  // namespace streams
