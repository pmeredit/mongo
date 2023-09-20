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
    return conf;
}

KafkaEmitOperator::KafkaEmitOperator(Context* context, Options options)
    : SinkOperator(context, /* numInputs */ 1), _options(std::move(options)) {
    _conf = createKafkaConf();

    std::string errstr;
    _producer.reset(RdKafka::Producer::create(_conf.get(), errstr));
    uassert(ErrorCodes::UnknownError,
            str::stream() << "Failed to create producer with error: " << errstr,
            _producer);
    _topic.reset(
        RdKafka::Topic::create(_producer.get(), _options.topicName, /*conf*/ nullptr, errstr));
    uassert(74689, str::stream() << "Failed to create topic with error: " << errstr, _topic);

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

void KafkaEmitOperator::processStreamDoc(const StreamDocument& streamDoc) {
    auto docAsStr = tojson(streamDoc.doc.toBson());
    auto docSize = docAsStr.size();

    constexpr int flags = RdKafka::Producer::RK_MSG_BLOCK /* block if queue is full */ |
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */;

    RdKafka::ErrorCode err =
        _producer->produce(_topic.get(),
                           _outputPartition,
                           flags,
                           const_cast<char*>(docAsStr.c_str()),
                           docSize,
                           nullptr /* key */,
                           0 /* key_len */,
                           nullptr /* Per-message opaque value passed to delivery report */);

    uassert(ErrorCodes::UnknownError,
            str::stream() << "Failed to emit to topic " << _options.topicName
                          << " due to error: " << err,
            err == RdKafka::ERR_NO_ERROR);
}

void KafkaEmitOperator::doStop() {
    doFlush();
}

void KafkaEmitOperator::doFlush() {
    if (!_producer) {
        return;
    }

    LOGV2_INFO(74685, "KafkaEmitOperator flush starting", "context"_attr = _context);
    auto err = _producer->flush(_options.flushTimeout.count());
    uassert(74686,
            fmt::format("Kafka $emit encountered error while flushing: {}", RdKafka::err2str(err)),
            err == RdKafka::ERR_NO_ERROR);
    LOGV2_INFO(74687, "KafkaEmitOperator flush complete", "context"_attr = _context);
}

};  // namespace streams
