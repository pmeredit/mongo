/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_partition_consumer.h"

namespace streams {

using namespace mongo;

KafkaConsumerOperator::KafkaConsumerOperator(Options options)
    : Operator(/*numInputs*/ 0, /*numOutputs*/ 1), _options(std::move(options)) {
    int32_t numPartitions = _options.partitionOptions.size();
    // Initialize _watermarkCombiner.
    _watermarkCombiner = std::make_unique<WatermarkCombiner>(/*numInputs*/ numPartitions);

    // Create KafkaPartitionConsumer instances, one for each partition.
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        const auto& partitionOptions = _options.partitionOptions[partition];
        ConsumerInfo consumerInfo;
        KafkaPartitionConsumer::Options options;
        options.bootstrapServers = _options.bootstrapServers;
        options.topicName = _options.topicName;
        options.partition = partitionOptions.partition;
        options.startOffset = partitionOptions.startOffset;
        options.deserializer = _options.deserializer;
        options.maxNumDocsToReturn = _options.maxNumDocsToReturn;
        options.maxNumDocsToPrefetch = 10 * _options.maxNumDocsToReturn;

        if (_options.isTest) {
            consumerInfo.consumer = std::make_unique<FakeKafkaPartitionConsumer>();
        } else {
            consumerInfo.consumer = std::make_unique<KafkaPartitionConsumer>(std::move(options));
        }
        consumerInfo.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
            /*inputIdx*/ partition,
            _watermarkCombiner.get(),
            partitionOptions.watermarkGeneratorAllowedLatenessMs);
        _consumers.push_back(std::move(consumerInfo));
    }
}

void KafkaConsumerOperator::doStart() {
    // Start all partition consumers.
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->init();
        consumerInfo.consumer->start();
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
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->stop();
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
                // TODO: add jitter
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

int32_t KafkaConsumerOperator::runOnce() {
    StreamDataMsg dataMsg;
    dataMsg.docs.reserve(2 * _options.maxNumDocsToReturn);

    int32_t numDocsFlushed{0};
    auto maybeFlush = [&](bool force) {
        if (force || int32_t(dataMsg.docs.size()) >= _options.maxNumDocsToReturn) {
            auto newControlMsg = boost::make_optional<StreamControlMsg>({});
            newControlMsg->watermarkMsg = _watermarkCombiner->getCombinedWatermarkMsg();
            if (*newControlMsg == _lastControlMsg) {
                newControlMsg = boost::none;
            } else {
                _lastControlMsg = *newControlMsg;
            }

            numDocsFlushed += dataMsg.docs.size();
            if (!dataMsg.docs.empty()) {
                sendDataMsg(/*outputIdx*/ 0, std::move(dataMsg), std::move(newControlMsg));
                dataMsg = StreamDataMsg{};
            } else if (newControlMsg) {
                // Note that we send newControlMsg only if it differs from _lastControlMsg.
                sendControlMsg(/*outputIdx*/ 0, std::move(*newControlMsg));
            }
        }
    };

    // TODO: Implement watermark alignment.
    // Get documents from each KafkaPartitionConsumerBase and send them to the output Operator.
    for (auto& consumerInfo : _consumers) {
        auto sourceDocs = consumerInfo.consumer->getDocuments();
        dassert(int32_t(sourceDocs.size()) <= _options.maxNumDocsToReturn);
        for (auto& sourceDoc : sourceDocs) {
            auto streamDoc =
                processSourceDocument(std::move(sourceDoc), consumerInfo.watermarkGenerator.get());
            if (streamDoc) {
                consumerInfo.watermarkGenerator->onEvent(streamDoc->minEventTimestampMs);
                dataMsg.docs.push_back(std::move(*streamDoc));
            }  // Else, the document was sent to the dead letter queue.
        }
        maybeFlush(/*force*/ false);
    }
    maybeFlush(/*force*/ true);

    return numDocsFlushed;
}

boost::optional<StreamDocument> KafkaConsumerOperator::processSourceDocument(
    KafkaSourceDocument sourceDoc, WatermarkGenerator* watermarkGenerator) {
    if (!sourceDoc.doc) {
        dassert(sourceDoc.docBuf);
        _options.deadLetterQueue->addMessage(std::move(sourceDoc));
        return boost::none;
    }

    boost::optional<StreamDocument> streamDoc;
    try {
        mongo::Date_t eventTimestamp;
        if (_options.timestampExtractor) {
            eventTimestamp =
                _options.timestampExtractor->extractTimestamp(Document(*sourceDoc.doc));
        } else {
            dassert(sourceDoc.logAppendTimeMs);
            eventTimestamp = Date_t::fromMillisSinceEpoch(*sourceDoc.logAppendTimeMs);
        }
        // Now we are destroying sourceDoc.doc, make sure that no exceptions related to
        // processing this document get thrown after this point.
        mongo::BSONObj bsonDoc = std::move(*sourceDoc.doc);
        sourceDoc.doc = boost::none;
        BSONObjBuilder objBuilder(std::move(bsonDoc));
        objBuilder.appendDate(_options.timestampOutputFieldName, eventTimestamp);

        streamDoc = StreamDocument(Document(objBuilder.obj()));
        streamDoc->minProcessingTimeMs = curTimeMillis64();
        streamDoc->minEventTimestampMs = eventTimestamp.toMillisSinceEpoch();
        streamDoc->maxEventTimestampMs = eventTimestamp.toMillisSinceEpoch();
    } catch (const std::exception& e) {
        LOGV2_ERROR(74675,
                    "{topicName}: encountered exception while processing a source "
                    "document: {error}",
                    "topicName"_attr = _options.topicName,
                    "error"_attr = e.what());
        if (streamDoc) {
            dassert(!sourceDoc.doc);
            sourceDoc.doc = streamDoc->doc.toBson();
        }
        streamDoc = boost::none;
        _options.deadLetterQueue->addMessage(std::move(sourceDoc));
        return boost::none;
    }

    dassert(streamDoc);
    if (_options.dlqOptions.dlqLateEvents &&
        watermarkGenerator->isLate(streamDoc->minEventTimestampMs)) {
        // Drop the document, send it to DLQ.
        dassert(!sourceDoc.doc);
        sourceDoc.doc = streamDoc->doc.toBson();
        streamDoc = boost::none;
        _options.deadLetterQueue->addMessage(std::move(sourceDoc));
        return boost::none;
    }
    return streamDoc;
}

}  // namespace streams
