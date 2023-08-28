/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/kafka_consumer_operator.h"

#include <optional>
#include <rdkafkacpp.h>

#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/json_event_deserializer.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/util.h"
#include "streams/exec/watermark_combiner.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

KafkaConsumerOperator::KafkaConsumerOperator(Context* context, Options options)
    : SourceOperator(context, /*numOutputs*/ 1), _options(std::move(options)) {
    if (_options.testOnlyNumPartitions) {
        invariant(_options.isTest);
        _numPartitions = _options.testOnlyNumPartitions;
    }
}

void KafkaConsumerOperator::doStart() {
    // We need to wait until a connection is established with the input source before we can start
    // the per-partition consumer instances.
}

void KafkaConsumerOperator::doStop() {
    // Stop all partition consumers.
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->stop();
    }
    _consumers.clear();
}

void KafkaConsumerOperator::fetchNumPartitions() {
    invariant(!_numPartitions);

    if (!_metadataConsumer) {
        // Create a KafkaPartitionConsumer instance just to determine the partition count for the
        // topic. Note that we will destroy this instance soon and won't be using it for reading
        // any input documents.
        _metadataConsumer = createPartitionConsumer(/*partition*/ 0, _options.startOffset);
        _metadataConsumer->consumer->init();
        _metadataConsumer->consumer->start();
    }

    _numPartitions = _metadataConsumer->consumer->getNumPartitions();
    if (_numPartitions) {
        invariant(*_numPartitions > 0);
        LOGV2_INFO(74703,
                   "Retrieved topic partition count",
                   "topicName"_attr = _options.topicName,
                   "numPartitions"_attr = *_numPartitions);
        _metadataConsumer->consumer->stop();
        _metadataConsumer.reset();
    }
}

void KafkaConsumerOperator::initFromCheckpoint() {
    invariant(_numPartitions);
    invariant(_consumers.empty());
    invariant(_context->restoreCheckpointId);

    // De-serialize and verify the state.
    boost::optional<mongo::BSONObj> bsonState = _context->checkpointStorage->readState(
        *_context->restoreCheckpointId, _operatorId, 0 /* chunkNumber */);
    CHECKPOINT_RECOVERY_ASSERT(
        *_context->restoreCheckpointId, _operatorId, "state chunk 0 should exist", bsonState);
    auto state =
        KafkaSourceCheckpointState::parseOwned(IDLParserContext(getName()), std::move(*bsonState));

    const auto& partitions = state.getPartitions();
    CHECKPOINT_RECOVERY_ASSERT(
        *_context->restoreCheckpointId,
        _operatorId,
        str::stream() << "partition count in the checkpoint (" << partitions.size() << ") "
                      << "does not match the partition count of Kafka topic (" << *_numPartitions
                      << ")",
        int64_t(partitions.size()) == *_numPartitions);

    // Create KafkaPartitionConsumer instances from the checkpoint data.
    _consumers.reserve(*_numPartitions);
    for (int32_t partition = 0; partition < *_numPartitions; ++partition) {
        // Note: when all partitions are not contiguous on one $source, we may need to modify this.
        const auto& partitionState = partitions[partition];
        CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                   _operatorId,
                                   str::stream()
                                       << "unexpected partitionState at index: " << partition
                                       << " partition: " << partitionState.getPartition(),
                                   partitionState.getPartition() == partition);

        // Create the consumer with the offset in the checkpoint.
        ConsumerInfo consumerInfo = createPartitionConsumer(partition, partitionState.getOffset());

        CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                   _operatorId,
                                   str::stream() << "state has unexpected watermark: "
                                                 << bool(partitionState.getWatermark()),
                                   bool(partitionState.getWatermark()) == _options.useWatermarks)
        if (_options.useWatermarks) {
            // Setup the watermark from the checkpoint.
            invariant(_watermarkCombiner);
            // All partition watermarks start as active when restoring from a checkpoint.
            WatermarkControlMsg watermark{WatermarkStatus::kActive,
                                          partitionState.getWatermark()->getEventTimeMs()};
            consumerInfo.watermarkGenerator =
                std::make_unique<DelayedWatermarkGenerator>(partition /* inputIdx */,
                                                            _watermarkCombiner.get(),
                                                            _options.allowedLatenessMs,
                                                            watermark);
        }
        _consumers.push_back(std::move(consumerInfo));
    }

    // Start all partition consumers.
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->init();
        consumerInfo.consumer->start();
    }
}

void KafkaConsumerOperator::initFromOptions() {
    invariant(_numPartitions);
    invariant(_consumers.empty());
    // Create KafkaPartitionConsumer instances, one for each partition.
    _consumers.reserve(*_numPartitions);
    for (int32_t partition = 0; partition < *_numPartitions; ++partition) {
        ConsumerInfo consumerInfo = createPartitionConsumer(partition, _options.startOffset);

        if (_options.useWatermarks) {
            invariant(_watermarkCombiner);
            consumerInfo.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
                partition /* inputIdx */, _watermarkCombiner.get(), _options.allowedLatenessMs);
            consumerInfo.idlenessTimeoutMs = _options.idlenessTimeoutMs;

            // Capturing the initial time during construction is useful in the context of idleness
            // detection as it provides a baseline for subsequent events to compare to.
            consumerInfo.lastEventReadTimestamp = stdx::chrono::steady_clock::now();
        }
        _consumers.push_back(std::move(consumerInfo));
    }

    // Start all partition consumers.
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->init();
        consumerInfo.consumer->start();
    }
}

void KafkaConsumerOperator::init() {
    invariant(_numPartitions);
    invariant(_consumers.empty());

    if (_options.useWatermarks) {
        invariant(!_watermarkCombiner);
        _watermarkCombiner = std::make_unique<WatermarkCombiner>(*_numPartitions);
    }

    if (_context->restoreCheckpointId) {
        initFromCheckpoint();
    } else {
        initFromOptions();
    }
    invariant(!_consumers.empty());
}

void KafkaConsumerOperator::doConnect() {
    if (!_consumers.empty()) {
        return;
    }

    if (!_numPartitions) {
        fetchNumPartitions();
    }

    if (_numPartitions) {
        // We successfully fetched the topic partition count.
        init();
    }
}

bool KafkaConsumerOperator::doIsConnected() {
    if (_consumers.empty()) {
        return false;
    }
    invariant(int64_t(_consumers.size()) == *_numPartitions);

    for (auto& consumerInfo : _consumers) {
        if (!consumerInfo.consumer->isConnected()) {
            return false;
        }
    }
    return true;
}

int64_t KafkaConsumerOperator::doRunOnce() {
    invariant(!_consumers.empty());

    StreamDataMsg dataMsg;
    dataMsg.docs.reserve(2 * _options.maxNumDocsToReturn);

    int64_t totalNumInputDocs{0};
    auto maybeFlush = [&](bool force) {
        if (force || int32_t(dataMsg.docs.size()) >= _options.maxNumDocsToReturn) {
            boost::optional<StreamControlMsg> newControlMsg = boost::none;
            if (_watermarkCombiner) {
                newControlMsg = StreamControlMsg{_watermarkCombiner->getCombinedWatermarkMsg()};
                if (*newControlMsg == _lastControlMsg) {
                    newControlMsg = boost::none;
                } else {
                    _lastControlMsg = *newControlMsg;
                }
            }

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

        // Handle idleness time out.
        if (consumerInfo.idlenessTimeoutMs != stdx::chrono::milliseconds(0)) {
            dassert(consumerInfo.watermarkGenerator);
            const auto currentTime = stdx::chrono::steady_clock::now();

            // Check for idleness if we didn't get any documents, otherwise, we record the current
            // time and set the current partition as active.
            if (sourceDocs.empty()) {
                dassert(currentTime > consumerInfo.lastEventReadTimestamp);
                const auto millisSinceLastEvent =
                    stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                        currentTime - consumerInfo.lastEventReadTimestamp);
                if (millisSinceLastEvent >= consumerInfo.idlenessTimeoutMs) {
                    consumerInfo.watermarkGenerator->setIdle();
                }
            } else {
                consumerInfo.lastEventReadTimestamp = currentTime;
                consumerInfo.watermarkGenerator->setActive();
            }
        }

        int64_t numInputDocs = sourceDocs.size();
        int64_t numInputBytes = 0;
        int64_t numDlqDocs = 0;

        totalNumInputDocs += numInputDocs;

        for (auto& sourceDoc : sourceDocs) {
            numInputBytes += sourceDoc.sizeBytes;
            invariant(!consumerInfo.maxOffset || sourceDoc.offset > *consumerInfo.maxOffset);
            consumerInfo.maxOffset = sourceDoc.offset;
            auto streamDoc =
                processSourceDocument(std::move(sourceDoc), consumerInfo.watermarkGenerator.get());
            if (streamDoc) {
                if (consumerInfo.watermarkGenerator) {
                    consumerInfo.watermarkGenerator->onEvent(streamDoc->minEventTimestampMs);
                }
                dataMsg.docs.push_back(std::move(*streamDoc));
            } else {
                // Invalid or late document, inserted into the dead letter queue.
                ++numDlqDocs;
            }
        }

        incOperatorStats(OperatorStats{
            .numInputDocs = numInputDocs,
            .numInputBytes = numInputBytes,
            .numDlqDocs = numDlqDocs,
        });

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
        _context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
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
        _context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
        return boost::none;
    }

    dassert(streamDoc);
    if (watermarkGenerator && watermarkGenerator->isLate(streamDoc->minEventTimestampMs)) {
        // Drop the document, send it to DLQ.
        dassert(!sourceDoc.doc);
        sourceDoc.doc = streamDoc->doc.toBson();
        sourceDoc.error = "Input document arrived late";
        streamDoc = boost::none;
        _context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
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
    invariant(_consumers.size() > 0);
    const size_t numPartitions = _consumers.size();
    for (size_t partition = 0; partition < numPartitions; partition++) {
        auto fakeKafkaPartition =
            dynamic_cast<FakeKafkaPartitionConsumer*>(_consumers[partition].consumer.get());
        uassert(ErrorCodes::InvalidOptions,
                "can only insert with FakeKafkaPartitionConsumer",
                bool(fakeKafkaPartition));

        std::vector<KafkaSourceDocument> docs;
        for (size_t j = partition; j < inputDocs.size(); j += numPartitions) {
            int64_t sizeBytes = inputDocs[j].objsize();
            docs.push_back(
                KafkaSourceDocument{.doc = std::move(inputDocs[j]),
                                    .sizeBytes = sizeBytes,
                                    .logAppendTimeMs = Date_t::now().toMillisSinceEpoch()});
        }
        fakeKafkaPartition->addDocuments(std::move(docs));
    }
}

KafkaConsumerOperator::ConsumerInfo KafkaConsumerOperator::createPartitionConsumer(
    int32_t partition, int64_t startOffset) {
    ConsumerInfo consumerInfo;
    KafkaPartitionConsumerBase::Options options;
    consumerInfo.partition = partition;
    options.bootstrapServers = _options.bootstrapServers;
    options.topicName = _options.topicName;
    options.partition = partition;
    options.deserializer = _options.deserializer;
    options.maxNumDocsToReturn = _options.maxNumDocsToReturn;
    options.maxNumDocsToPrefetch = 10 * _options.maxNumDocsToReturn;
    options.startOffset = startOffset;
    options.authConfig = _options.authConfig;
    options.kafkaRequestTimeoutMs = _options.kafkaRequestTimeoutMs;
    options.kafkaRequestFailureSleepDurationMs = _options.kafkaRequestFailureSleepDurationMs;
    if (_options.isTest) {
        consumerInfo.consumer = std::make_unique<FakeKafkaPartitionConsumer>(std::move(options));
    } else {
        consumerInfo.consumer = std::make_unique<KafkaPartitionConsumer>(std::move(options));
    }
    return consumerInfo;
}

void KafkaConsumerOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    invariant(controlMsg.checkpointMsg && !controlMsg.watermarkMsg);
    processCheckpointMsg(controlMsg);
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

void KafkaConsumerOperator::processCheckpointMsg(const StreamControlMsg& controlMsg) {
    invariant(controlMsg.checkpointMsg);
    invariant(!_consumers.empty());
    std::vector<KafkaPartitionCheckpointState> partitions;
    partitions.reserve(_consumers.size());
    for (const ConsumerInfo& consumerInfo : _consumers) {
        int64_t checkpointStartingOffset{0};
        if (consumerInfo.maxOffset) {
            checkpointStartingOffset = *consumerInfo.maxOffset + 1;
        } else {
            auto consumerStartOffset = consumerInfo.consumer->getStartOffset();
            invariant(consumerStartOffset);
            checkpointStartingOffset = *consumerStartOffset;
        }
        KafkaPartitionCheckpointState partitionState{consumerInfo.partition,
                                                     checkpointStartingOffset};
        if (_options.useWatermarks) {
            partitionState.setWatermark(WatermarkState{
                consumerInfo.watermarkGenerator->getWatermarkMsg().eventTimeWatermarkMs});
        }
        partitions.push_back(std::move(partitionState));
    }

    KafkaSourceCheckpointState state{std::move(partitions)};
    LOGV2_INFO(77177,
               "KafkaConsumerOperator adding state to checkpoint",
               "state"_attr = state.toBSON().toString(),
               "checkpointId"_attr = controlMsg.checkpointMsg->id);
    _context->checkpointStorage->addState(
        controlMsg.checkpointMsg->id, _operatorId, std::move(state).toBSON(), 0 /* chunkNumber */);
}

}  // namespace streams
