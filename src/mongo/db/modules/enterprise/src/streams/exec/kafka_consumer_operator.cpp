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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

KafkaConsumerOperator::KafkaConsumerOperator(Context* context, Options options)
    : SourceOperator(context, /*numOutputs*/ 1), _options(std::move(options)) {
    int32_t numPartitions = _options.partitionOptions.size();
    if (_options.useWatermarks) {
        _watermarkCombiner = std::make_unique<WatermarkCombiner>(numPartitions);
    }

    // Create KafkaPartitionConsumer instances, one for each partition.
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        const auto& partitionOptions = _options.partitionOptions[partition];
        ConsumerInfo consumerInfo =
            createPartitionConsumer(partition, partitionOptions.startOffset);

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
            if (_watermarkCombiner) {
                newControlMsg = StreamControlMsg{_watermarkCombiner->getCombinedWatermarkMsg()};
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
            invariant(sourceDoc.offset > consumerInfo.maxOffset);
            consumerInfo.maxOffset = sourceDoc.offset;
            auto streamDoc =
                processSourceDocument(std::move(sourceDoc), consumerInfo.watermarkGenerator.get());
            if (streamDoc) {
                if (consumerInfo.watermarkGenerator) {
                    consumerInfo.watermarkGenerator->onEvent(streamDoc->minEventTimestampMs);
                }
                dataMsg.docs.push_back(std::move(*streamDoc));
            }  // Else, the document was sent to the dead letter queue.
        }

        // Handle idleness time out.
        if (consumerInfo.idlenessTimeoutMs != stdx::chrono::milliseconds(0)) {
            dassert(consumerInfo.watermarkGenerator);
            const auto currentTime = stdx::chrono::steady_clock::now();
            if (!sourceDocs.empty()) {
                consumerInfo.lastEventReadTimestamp = currentTime;
                consumerInfo.watermarkGenerator->setActive();
            } else {
                dassert(currentTime > consumerInfo.lastEventReadTimestamp);
                const auto millisSinceLastEvent =
                    stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                        currentTime - consumerInfo.lastEventReadTimestamp);
                if (millisSinceLastEvent >= consumerInfo.idlenessTimeoutMs) {
                    consumerInfo.watermarkGenerator->setIdle();
                }
            }
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
    consumerInfo.maxOffset = options.startOffset - 1;
    options.authConfig = _options.authConfig;
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
    std::vector<KafkaPartitionCheckpointState> partitions;
    partitions.reserve(_consumers.size());
    for (const ConsumerInfo& consumerInfo : _consumers) {
        // TODO(SERVER-78500): Handle typical "first start" case where supplied log offset is
        // OFFSET_END or OFFSET_BEGINNING.
        int64_t checkpointStartingOffset{0};
        if (consumerInfo.maxOffset >= 0) {
            checkpointStartingOffset = consumerInfo.maxOffset + 1;
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
    LOGV2_INFO(74703,
               "KafkaConsumerOperator adding state to checkpoint",
               "state"_attr = state.toBSON().toString(),
               "checkpointId"_attr = controlMsg.checkpointMsg->id);
    _context->checkpointStorage->addState(
        controlMsg.checkpointMsg->id, _operatorId, std::move(state).toBSON(), 0 /* chunkNumber */);
}

void KafkaConsumerOperator::doRestoreFromCheckpoint(CheckpointId checkpointId) {
    // De-serialize and verify the state.
    boost::optional<mongo::BSONObj> bsonState =
        _context->checkpointStorage->readState(checkpointId, _operatorId, 0 /* chunkNumber */);
    CHECKPOINT_RECOVERY_ASSERT(checkpointId, _operatorId, "state chunk 0 should exist", bsonState);
    auto state =
        KafkaSourceCheckpointState::parseOwned(IDLParserContext(getName()), std::move(*bsonState));
    std::vector<KafkaPartitionCheckpointState>& partitions = state.getPartitions();
    CHECKPOINT_RECOVERY_ASSERT(checkpointId,
                               _operatorId,
                               str::stream()
                                   << "unexpected partition count of: " << partitions.size()
                                   << ", should be: " << _consumers.size(),
                               partitions.size() == _consumers.size());

    // Create KafkaPartitionConsumer instances from the checkpoint data.
    // Clear the _consumers created in the constructor.
    _consumers.clear();

    int32_t numPartitions = _options.partitionOptions.size();
    for (int32_t partition = 0; partition < numPartitions; ++partition) {
        // Note: when all partitions are not contiguous on one $source, we may need to modify this.
        KafkaPartitionCheckpointState& partitionState = partitions[partition];
        CHECKPOINT_RECOVERY_ASSERT(checkpointId,
                                   _operatorId,
                                   str::stream()
                                       << "unexpected partitionState at index: " << partition
                                       << " partition: " << partitionState.getPartition(),
                                   partitionState.getPartition() == partition);

        // Create the consumer with the offset in the checkpoint.
        ConsumerInfo consumerInfo = createPartitionConsumer(partition, partitionState.getOffset());

        CHECKPOINT_RECOVERY_ASSERT(checkpointId,
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
}

}  // namespace streams
