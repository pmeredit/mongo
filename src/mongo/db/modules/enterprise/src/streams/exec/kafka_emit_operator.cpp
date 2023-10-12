/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <limits>
#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>

#include "streams/exec/kafka_emit_operator.h"

#include "mongo/platform/basic.h"
#include "mongo/util/str.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/log_util.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

std::unique_ptr<RdKafka::Conf> KafkaEmitOperator::createKafkaConf() {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto setConf = [confPtr = conf.get()](const std::string& confName,
                                          const std::string& confValue) {
        std::string errstr;
        if (confPtr->set(confName, confValue, errstr) != RdKafka::Conf::CONF_OK) {
            uasserted(ErrorCodes::UnknownError,
                      str::stream() << "Failed while setting configuration " << confName
                                    << " with error: " << errstr);
        }
    };
    setConf("bootstrap.servers", _options.bootstrapServers);
    // Do not log broker disconnection messages.
    setConf("log.connection.close", "false");
    // Do not refresh topic or broker metadata.
    setConf("topic.metadata.refresh.interval.ms", "-1");
    // Set auth related configurations.
    for (const auto& config : _options.authConfig) {
        setConf(config.first, config.second);
    }

    // Configure the underlying kafka producer queue with sensible defaults. In particular:
    // - We wish to allow up to one second for events to accumulate (this reduces the overhead for
    // sending messages to our broker).
    // - We want to have a relatively low memory footprint, so allow our queue to buffer up to 16MB
    // of data (or, 16384KB).
    // - Finally, we configure the maximum number of documents to the default, which is 100k. We
    // don't expect to hit this as this is relatively high compared to the maximum memory limit.
    setConf("queue.buffering.max.ms", "1000");
    setConf("queue.buffering.max.kbytes", "16384");
    setConf("queue.buffering.max.messages", "100000");
    // This is the maximum time librdkafka may use to deliver a message (including retries).
    // Set to 10 seconds.
    setConf("message.timeout.ms", "30000");
    return conf;
}

KafkaEmitOperator::KafkaEmitOperator(Context* context, Options options)
    : SinkOperator(context, /* numInputs */ 1),
      _options(std::move(options)),
      _expCtx(context->expCtx) {
    _conf = createKafkaConf();

    std::string errstr;
    _producer.reset(RdKafka::Producer::create(_conf.get(), errstr));
    uassert(ErrorCodes::UnknownError,
            str::stream() << "Failed to create producer with error: " << errstr,
            _producer);
    if (_options.testOnlyPartition) {
        _outputPartition = *_options.testOnlyPartition;
    }
}

void KafkaEmitOperator::doSinkOnDataMsg(int32_t inputIdx,
                                        StreamDataMsg dataMsg,
                                        boost::optional<StreamControlMsg> controlMsg) {
    for (auto& streamDoc : dataMsg.docs) {
        try {
            processStreamDoc(streamDoc);
        } catch (const DBException& e) {
            std::string error = str::stream() << "Failed to process input document in " << getName()
                                              << " with error: " << e.what();
            _context->dlq->addMessage(toDeadLetterQueueMsg(streamDoc, std::move(error)));
        }
    }
}

namespace {
static constexpr size_t kMaxTopicNamesCacheSize = 100;
}

void KafkaEmitOperator::processStreamDoc(const StreamDocument& streamDoc) {
    auto docAsStr = tojson(streamDoc.doc.toBson());
    auto docSize = docAsStr.size();

    constexpr int flags = RdKafka::Producer::RK_MSG_BLOCK /* block if queue is full */ |
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */;

    auto topicName = _options.topicName.isLiteral()
        ? _options.topicName.getLiteral()
        : _options.topicName.evaluate(_expCtx.get(), streamDoc.doc);
    auto topicIt = _topicCache.find(topicName);
    if (topicIt == _topicCache.cend()) {
        uassert(8117202,
                "Too many unique topic names: {}"_format(_topicCache.size()),
                _topicCache.size() < kMaxTopicNamesCacheSize);

        std::string errstr;
        std::unique_ptr<RdKafka::Topic> topic{RdKafka::Topic::create(_producer.get(),
                                                                     topicName,
                                                                     /*conf*/ nullptr,
                                                                     errstr)};
        uassert(8117200, "Failed to create topic with error: {}"_format(errstr), topic);
        bool inserted = false;
        std::tie(topicIt, inserted) = _topicCache.emplace(topicName, std::move(topic));
        uassert(8117201, "Failed to insert a new topic {}"_format(topicName), inserted);
    }

    // TODO(SERVER-80742): Validate the connection is still established.
    // This call to produce will succeed even if the actual connection to Kafka is down.
    RdKafka::ErrorCode err =
        _producer->produce(topicIt->second.get(),
                           _outputPartition,
                           flags,
                           const_cast<char*>(docAsStr.c_str()),
                           docSize,
                           nullptr /* key */,
                           0 /* key_len */,
                           nullptr /* Per-message opaque value passed to delivery report */);
    uassert(ErrorCodes::UnknownError,
            "Failed to emit to topic {} due to error: {}"_format(topicName, err),
            err == RdKafka::ERR_NO_ERROR);
}

void KafkaEmitOperator::doStop() {
    doFlush();
}

void KafkaEmitOperator::doStart() {
    // Validate that the connection can be established by querying metadata.
    // TODO(SERVER-81550): Move this connection test to a background thread in doConnect.
    // Right now we make synchronous requests to the metadata, which will slow down start.
    RdKafka::Metadata* metadata{nullptr};
    RdKafka::ErrorCode error{RdKafka::ERR_NO_ERROR};
    if (_options.topicName.isLiteral()) {
        std::string errstr;
        std::unique_ptr<RdKafka::Topic> topic{
            RdKafka::Topic::create(_producer.get(),
                                   _options.topicName.getLiteral(),
                                   /*conf*/ nullptr,
                                   errstr)};
        uassert(8117204, "$emit to Kafka failed to connect to topic.", topic);
        error = _producer->metadata(
            false /* all_topics */, topic.get(), &metadata, _options.metadataQueryTimeout.count());
    } else {
        error = _producer->metadata(
            true /* all_topics */, nullptr, &metadata, _options.metadataQueryTimeout.count());
    }
    std::unique_ptr<RdKafka::Metadata> deleter(metadata);
    uassert(8141700,
            "$emit to Kafka encountered error while connecting, kafka error code: {}"_format(error),
            error == RdKafka::ERR_NO_ERROR);
}

void KafkaEmitOperator::doFlush() {
    if (!_producer) {
        return;
    }

    LOGV2_INFO(74685, "KafkaEmitOperator flush starting", "context"_attr = _context);
    auto err = _producer->flush(_options.flushTimeout.count());
    uassert(
        74686,
        fmt::format("$emit to Kafka encountered error while flushing, kafka error code: {}", err),
        err == RdKafka::ERR_NO_ERROR);
    LOGV2_INFO(74687, "KafkaEmitOperator flush complete", "context"_attr = _context);
}

};  // namespace streams
