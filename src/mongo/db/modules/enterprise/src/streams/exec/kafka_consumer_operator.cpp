/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <optional>

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/message.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

KafkaConsumerOperator::KafkaConsumerOperator(Options options)
    : SourceOperator(/*numInputs*/ 0, /*numOutputs*/ 1), _options(std::move(options)) {
    int32_t numPartitions = _options.partitionOptions.size();

    // Create KafkaPartitionConsumer instances, one for each partition.
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        const auto& partitionOptions = _options.partitionOptions[partition];
        ConsumerInfo consumerInfo;
        KafkaPartitionConsumer::Options partitionConsumerOptions;
        partitionConsumerOptions.bootstrapServers = _options.bootstrapServers;
        partitionConsumerOptions.topicName = _options.topicName;
        partitionConsumerOptions.partition = partitionOptions.partition;
        partitionConsumerOptions.startOffset = partitionOptions.startOffset;
        partitionConsumerOptions.deserializer = _options.deserializer;
        partitionConsumerOptions.maxNumDocsToReturn = _options.maxNumDocsToReturn;
        partitionConsumerOptions.maxNumDocsToPrefetch = 10 * _options.maxNumDocsToReturn;

        if (_options.isTest) {
            consumerInfo.consumer = std::make_unique<FakeKafkaPartitionConsumer>();
        } else {
            consumerInfo.consumer =
                std::make_unique<KafkaPartitionConsumer>(std::move(partitionConsumerOptions));
        }
        // Both combiner and generator must be set, or both must be unset.
        dassert(bool(_options.watermarkCombiner) == bool(partitionOptions.watermarkGenerator));
        consumerInfo.watermarkGenerator =
            _options.partitionOptions[partition].watermarkGenerator.get();
        _consumers.push_back(std::move(consumerInfo));
    }
}

void KafkaConsumerOperator::doStart() {
    // Start all partition consumers.
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->init();
        consumerInfo.consumer->start();
    }
}

void KafkaConsumerOperator::doStop() {
    // Stop all partition consumers.
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->stop();
    }
    _consumers.clear();
}

int32_t KafkaConsumerOperator::doRunOnce() {
    StreamDataMsg dataMsg;
    dataMsg.docs.reserve(2 * _options.maxNumDocsToReturn);

    int32_t totalNumInputDocs{0};
    int64_t numInputBytes{0};
    auto maybeFlush = [&](bool force) {
        if (force || int32_t(dataMsg.docs.size()) >= _options.maxNumDocsToReturn) {
            boost::optional<StreamControlMsg> newControlMsg = boost::none;
            if (_options.watermarkCombiner) {
                newControlMsg =
                    StreamControlMsg{_options.watermarkCombiner->getCombinedWatermarkMsg()};
                if (*newControlMsg == _lastControlMsg) {
                    newControlMsg = boost::none;
                } else {
                    _lastControlMsg = *newControlMsg;
                }
            }

            totalNumInputDocs += dataMsg.docs.size();
            incOperatorStats(OperatorStats{.numInputDocs = int64_t(dataMsg.docs.size()),
                                           .numInputBytes = numInputBytes});
            numInputBytes = 0;

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
            numInputBytes += sourceDoc.sizeBytes;
            auto streamDoc =
                processSourceDocument(std::move(sourceDoc), consumerInfo.watermarkGenerator);
            if (streamDoc) {
                if (consumerInfo.watermarkGenerator) {
                    consumerInfo.watermarkGenerator->onEvent(streamDoc->minEventTimestampMs);
                }
                dataMsg.docs.push_back(std::move(*streamDoc));
            }  // Else, the document was sent to the dead letter queue.
        }
        maybeFlush(/*force*/ false);
    }
    maybeFlush(/*force*/ true);

    return totalNumInputDocs;
}

boost::optional<StreamDocument> KafkaConsumerOperator::processSourceDocument(
    KafkaSourceDocument sourceDoc, WatermarkGenerator* watermarkGenerator) {
    if (!sourceDoc.doc) {
        dassert(sourceDoc.error);
        // Input document could not be successfully parsed, send it to DLQ.
        _options.context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
        return boost::none;
    }
    dassert(!sourceDoc.error);

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
        streamDoc->streamMeta.setSourceType(StreamMetaSourceTypeEnum::Kafka);
        streamDoc->streamMeta.setSourcePartition(sourceDoc.partition);
        streamDoc->streamMeta.setSourceOffset(sourceDoc.offset);
        if (sourceDoc.logAppendTimeMs) {
            streamDoc->streamMeta.setTimestamp(
                Date_t::fromMillisSinceEpoch(*sourceDoc.logAppendTimeMs));
        }
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
        sourceDoc.error = str::stream()
            << "Failed to process input document with error: " << e.what();
        streamDoc = boost::none;
        // Input document could not be successfully processed, send it to DLQ.
        _options.context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
        return boost::none;
    }

    dassert(streamDoc);
    if (watermarkGenerator && watermarkGenerator->isLate(streamDoc->minEventTimestampMs)) {
        // Drop the document, send it to DLQ.
        dassert(!sourceDoc.doc);
        sourceDoc.doc = streamDoc->doc.toBson();
        sourceDoc.error = "Input document arrived late";
        streamDoc = boost::none;
        _options.context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
        return boost::none;
    }
    return streamDoc;
}

BSONObjBuilder KafkaConsumerOperator::toDeadLetterQueueMsg(KafkaSourceDocument sourceDoc) {
    StreamMeta streamMeta;
    streamMeta.setSourceType(StreamMetaSourceTypeEnum::Kafka);
    streamMeta.setSourcePartition(sourceDoc.partition);
    streamMeta.setSourceOffset(sourceDoc.offset);
    BSONObjBuilder objBuilder =
        streams::toDeadLetterQueueMsg(std::move(streamMeta), std::move(sourceDoc.error));
    if (sourceDoc.doc) {
        objBuilder.append("fullDocument", std::move(*sourceDoc.doc));
    }
    return objBuilder;
}

void KafkaConsumerOperator::testOnlyInsertDocuments(std::vector<mongo::BSONObj> inputDocs) {
    dassert(_consumers.size() > 0);
    const size_t partitionCount = _consumers.size();
    for (size_t partition = 0; partition < partitionCount; partition++) {
        auto fakeKafkaPartition =
            dynamic_cast<FakeKafkaPartitionConsumer*>(_consumers[partition].consumer.get());
        uassert(ErrorCodes::InvalidOptions,
                "can only insert with FakeKafkaPartitionConsumer",
                bool(fakeKafkaPartition));

        std::vector<KafkaSourceDocument> docs;
        for (size_t j = partition; j < inputDocs.size(); j += partitionCount) {
            docs.push_back(
                KafkaSourceDocument{.doc = std::move(inputDocs[j]),
                                    .logAppendTimeMs = Date_t::now().toMillisSinceEpoch()});
        }
        fakeKafkaPartition->addDocuments(std::move(docs));
    }
}


}  // namespace streams
