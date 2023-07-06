/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>

#include "streams/exec/kafka_emit_operator.h"

#include "mongo/platform/basic.h"
#include "mongo/util/str.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/util.h"

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
    // TODO SERVER-78645: Set more config options that could be useful.
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
    auto ts = streamDoc.minEventTimestampMs;
    auto docAsStr = streamDoc.doc.toBson().toString();
    auto docSize = docAsStr.size();

    RdKafka::ErrorCode err =
        _producer->produce(_options.topicName,
                           RdKafka::Topic::PARTITION_UA /* Produce to any partition */,
                           RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                           const_cast<char*>(docAsStr.c_str()),
                           docSize,
                           nullptr /* key */,
                           0 /* key_len */,
                           ts /* timestamp */,
                           nullptr /* Message headers, if any */,
                           nullptr /* Per-message opaque value passed to delivery report */);

    uassert(ErrorCodes::UnknownError,
            str::stream() << "Failed to emit to topic " << _options.topicName
                          << " due to error: " << err,
            err == RdKafka::ERR_NO_ERROR);
}

};  // namespace streams
