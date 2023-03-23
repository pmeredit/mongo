/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_partition_consumer.h"

namespace streams {

using namespace mongo;

KafkaConsumerOperator::KafkaConsumerOperator(Options options)
    : Operator(/*numInputs*/ 0, /*numOutputs*/ 1), _options(std::move(options)) {
    // Create KafkaPartitionConsumer instances, one for each partition.
    int32_t numPartitions = _options.partitionOptions.size();
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        KafkaPartitionConsumer::Options options;
        options.bootstrapServers = _options.bootstrapServers;
        options.topicName = _options.topicName;
        options.partition = partition;
        options.deserializer = _options.deserializer;
        options.maxNumDocsToReturn = _options.maxNumDocsToReturn;
        options.maxNumDocsToPrefetch = 10 * _options.maxNumDocsToReturn;
        auto consumer = std::make_unique<KafkaPartitionConsumer>(std::move(options));
        _consumers.push_back(std::move(consumer));
    }
}

void KafkaConsumerOperator::doStart() {
    // Start all partition consumers.
    for (auto& consumer : _consumers) {
        consumer->init();
        consumer->start();
    }

    // Start the source thread.
    dassert(!_sourceThread.joinable());
    _sourceThread = stdx::thread([this] { sourceLoop(); });
}

void KafkaConsumerOperator::doStop() {
    // Stop the source thread.
    if (_sourceThread.joinable()) {
        stdx::lock_guard<Latch> lock(_mutex);
        _shutdown = true;
    }
    // Wait for the source thread to exit.
    _sourceThread.join();

    // Stop all partition consumers.
    for (auto& consumer : _consumers) {
        consumer->stop();
    }
    _consumers.clear();
}

void KafkaConsumerOperator::sourceLoop() {
    while (true) {
        {
            stdx::lock_guard<Latch> lock(_mutex);
            if (_shutdown) {
                LOGV2_INFO(74674,
                           "{topicName}: exiting sourceLoop()",
                           "topicName"_attr = _options.topicName);
                break;
            }
        }

        try {
            bool docsFlushed = runOnce();
            if (!docsFlushed) {
                // No docs were flushed in this run, so sleep a little before starting
                // the next run.
                stdx::this_thread::sleep_for(
                    stdx::chrono::milliseconds(_options.sourceIdleSleepDurationMs));
            }
        } catch (const std::exception& e) {
            // TODO: Propagate this error to the higher layer and also deschedule the stream.
            LOGV2_ERROR(74676,
                        "{topicName}: encountered exception, exiting sourceLoop(): {error}",
                        "topicName"_attr = _options.topicName,
                        "error"_attr = e.what());
            break;
        }
    }
}

bool KafkaConsumerOperator::runOnce() {
    StreamDataMsg dataMsg;
    dataMsg.docs.reserve(2 * _options.maxNumDocsToReturn);

    int32_t numDocsFlushed{0};
    auto maybeFlush = [&](bool force) {
        if (force || int32_t(dataMsg.docs.size()) >= _options.maxNumDocsToReturn) {
            numDocsFlushed += dataMsg.docs.size();
            sendDataMsg(/*outputIdx*/ 0, std::move(dataMsg), /*controlMsg*/ boost::none);
            dataMsg = StreamDataMsg{};
        }
    };

    // TODO: Use WatermarkGenerator and WatermarkCombiner to generate watermarks.
    // TODO: Implement watermark alignment.
    // Get documents from each KafkaPartitionConsumer and send them to the output Operator.
    for (auto& consumer : _consumers) {
        auto docs = consumer->getDocuments();
        for (auto& doc : docs) {
            dassert(int32_t(docs.size()) <= _options.maxNumDocsToReturn);
            // TODO: Extract event time from the document populate it in _ts field.
            StreamDocument streamDoc(Document(std::move(doc.doc)));
            streamDoc.minIngestionTimeMs = doc.logAppendTimeMs;
            dataMsg.docs.push_back(std::move(streamDoc));
        }
        maybeFlush(/*force*/ false);
    }

    if (!dataMsg.docs.empty()) {
        maybeFlush(/*force*/ true);
    }

    return numDocsFlushed > 0;
}

}  // namespace streams
