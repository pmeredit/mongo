/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/kafka_consumer_operator.h"

#include <fmt/format.h>
#include <optional>
#include <rdkafka.h>
#include <rdkafkacpp.h>

#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
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
#include "streams/exec/old_checkpoint_storage.h"
#include "streams/exec/stream_stats.h"
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
    // Stop the metadata consumer if needed.
    if (_metadataConsumer && _metadataConsumer->consumer) {
        _metadataConsumer->consumer->stop();
        _metadataConsumer->consumer.reset();
    }

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
                   "context"_attr = _context,
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
    boost::optional<mongo::BSONObj> bsonState;
    if (_context->oldCheckpointStorage) {
        bsonState = _context->oldCheckpointStorage->readState(
            *_context->restoreCheckpointId, _operatorId, 0 /* chunkNumber */);
    } else {
        invariant(_context->checkpointStorage);
        auto reader = _context->checkpointStorage->createStateReader(*_context->restoreCheckpointId,
                                                                     _operatorId);
        auto record = _context->checkpointStorage->getNextRecord(reader.get());
        CHECKPOINT_RECOVERY_ASSERT(
            *_context->restoreCheckpointId, _operatorId, "state should exist", record);
        bsonState = record->toBson();
    }

    CHECKPOINT_RECOVERY_ASSERT(
        *_context->restoreCheckpointId, _operatorId, "state chunk 0 should exist", bsonState);
    auto state =
        KafkaSourceCheckpointState::parseOwned(IDLParserContext(getName()), std::move(*bsonState));
    LOGV2_INFO(77187,
               "KafkaConsumerOperator restoring from checkpoint",
               "state"_attr = state.toBSON(),
               "checkpointId"_attr = *_context->restoreCheckpointId);

    const auto& partitions = state.getPartitions();
    CHECKPOINT_RECOVERY_ASSERT(
        *_context->restoreCheckpointId,
        _operatorId,
        str::stream() << "partition count in the checkpoint (" << partitions.size() << ") "
                      << "does not match the partition count of Kafka topic (" << *_numPartitions
                      << ")",
        int64_t(partitions.size()) == *_numPartitions);

    // Use the consumer group ID from the checkpoint. The consumer group ID should never change
    // during the lifetime of a stream processor. The consumer group ID is only optional in the
    // checkpoint data for backwards compatibility reasons.
    if (auto consumerGroupId = state.getConsumerGroupId(); consumerGroupId) {
        _options.consumerGroupId = std::string(*consumerGroupId);
    }

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
            consumerInfo.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
                partition /* inputIdx */, _watermarkCombiner.get(), watermark);
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

    auto committedOffsets = getCommittedOffsets();
    invariant(committedOffsets.empty() || int64_t(committedOffsets.size()) == *_numPartitions);
    for (int32_t partition = 0; partition < *_numPartitions; ++partition) {
        int64_t startOffset = _options.startOffset;
        if (!committedOffsets.empty()) {
            startOffset = committedOffsets[partition];
        }

        ConsumerInfo consumerInfo = createPartitionConsumer(partition, startOffset);

        if (_options.useWatermarks) {
            invariant(_watermarkCombiner);
            consumerInfo.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
                partition /* inputIdx */, _watermarkCombiner.get());
            consumerInfo.partitionIdleTimeoutMs = _options.partitionIdleTimeoutMs;

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

ConnectionStatus KafkaConsumerOperator::doGetConnectionStatus() {
    if (_consumers.empty()) {
        if (_metadataConsumer && _metadataConsumer->consumer) {
            auto status = _metadataConsumer->consumer->getConnectionStatus();
            if (status.status == ConnectionStatus::Status::kError) {
                return status;
            }
        }
        return ConnectionStatus{ConnectionStatus::Status::kConnecting};
    }

    // Check if all the consumers are connected.
    for (auto& consumerInfo : _consumers) {
        auto status = consumerInfo.consumer->getConnectionStatus();
        if (!status.isConnected()) {
            return status;
        }
    }

    // All consumers are connected.
    return ConnectionStatus{ConnectionStatus::Status::kConnected};
}

int64_t KafkaConsumerOperator::doRunOnce() {
    invariant(!_consumers.empty());

    StreamDataMsg dataMsg;
    dataMsg.docs.reserve(2 * _options.maxNumDocsToReturn);

    // Priority queue that contains indices to the corresponding consumer in the `_consumers`
    // slice. The consumers in this priority queue are prioritized based on local watermarks,
    // so consumers with the lowest local watermark is prioritized over all other consumers.
    // This is so that consumers with lower local watermarks are prioritized first to avoid
    // unbounded growth of stateful operators in case one consumer's local watermark is far
    // ahead than the rest and another consumer's local watermark is preventing the global
    // watermark from advancing.
    auto cmp = [&](int a, int b) -> bool {
        if (_options.useWatermarks) {
            return _consumers[a].watermarkGenerator->getWatermarkMsg().eventTimeWatermarkMs >
                _consumers[b].watermarkGenerator->getWatermarkMsg().eventTimeWatermarkMs;
        }
        return false;
    };
    std::priority_queue<int, std::vector<int>, decltype(cmp)> consumerq(std::move(cmp));
    for (int i = 0; i < int(_consumers.size()); i++) {
        consumerq.push(i);
    }

    int64_t totalNumInputDocs{0};
    auto maybeFlush = [&](bool force) {
        if (force || int32_t(dataMsg.docs.size()) >= _options.maxNumDocsToReturn) {
            boost::optional<StreamControlMsg> newControlMsg = boost::none;
            if (_watermarkCombiner) {
                newControlMsg = StreamControlMsg{_watermarkCombiner->getCombinedWatermarkMsg()};
                if (*newControlMsg == _lastControlMsg) {
                    // Don't resend the same watermark.
                    newControlMsg = boost::none;
                }

                if (totalNumInputDocs == 0 && _options.sendIdleMessages) {
                    // If _options.sendIdleMessages is set, always send a kIdle watermark when
                    // there are 0 docs read from the source.
                    newControlMsg =
                        StreamControlMsg{.watermarkMsg = WatermarkControlMsg{
                                             .watermarkStatus = WatermarkStatus::kIdle}};
                }
            }

            if (!dataMsg.docs.empty()) {
                if (newControlMsg) {
                    _lastControlMsg = *newControlMsg;
                }
                sendDataMsg(/*outputIdx*/ 0, std::move(dataMsg), std::move(newControlMsg));
                dataMsg = StreamDataMsg{};
            } else if (newControlMsg) {
                _lastControlMsg = *newControlMsg;
                // Note that we send newControlMsg only if it differs from _lastControlMsg.
                sendControlMsg(/*outputIdx*/ 0, std::move(*newControlMsg));
            }
        }
    };

    // Max number of batches to process on this single run instance.
    int numBatches{0};
    int maxBatches = int(_consumers.size());

    // Only count as a processed batch if the consumer partition had documents to process,
    // otherwise don't add the consumer partition back into the `consumerq` priority queue.
    while (numBatches < maxBatches && !consumerq.empty()) {
        // Prioritize consumers with the lowest local watermark.
        int consumerIdx = consumerq.top();
        consumerq.pop();
        auto& consumerInfo = _consumers[consumerIdx];

        auto sourceDocs = consumerInfo.consumer->getDocuments();
        dassert(int32_t(sourceDocs.size()) <= _options.maxNumDocsToReturn);

        // Handle idleness time out.
        if (consumerInfo.partitionIdleTimeoutMs != stdx::chrono::milliseconds(0)) {
            dassert(consumerInfo.watermarkGenerator);
            const auto currentTime = stdx::chrono::steady_clock::now();

            // Check for idleness if we didn't get any documents, otherwise, we record the current
            // time and set the current partition as active.
            if (sourceDocs.empty()) {
                dassert(currentTime > consumerInfo.lastEventReadTimestamp);
                const auto millisSinceLastEvent =
                    stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                        currentTime - consumerInfo.lastEventReadTimestamp);
                if (millisSinceLastEvent >= consumerInfo.partitionIdleTimeoutMs) {
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

        if (numInputDocs > 0) {
            ++numBatches;
            consumerq.push(consumerIdx);
        }

        incOperatorStats(OperatorStats{.numInputDocs = numInputDocs,
                                       .numInputBytes = numInputBytes,
                                       .numDlqDocs = numDlqDocs});
        if (_watermarkCombiner) {
            _stats.watermark = _watermarkCombiner->getCombinedWatermarkMsg().eventTimeWatermarkMs;
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
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
        incOperatorStats({.numDlqBytes = numDlqBytes});
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
                    "context"_attr = _context,
                    "error"_attr = e.what());
        if (streamDoc) {
            dassert(!sourceDoc.doc);
            sourceDoc.doc = streamDoc->doc.toBson();
        }
        sourceDoc.error = str::stream()
            << "Failed to process input document with error: " << e.what();
        streamDoc = boost::none;
        // Input document could not be successfully processed, send it to DLQ.
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(std::move(sourceDoc)));
        incOperatorStats({.numDlqBytes = numDlqBytes});
        return boost::none;
    }

    dassert(streamDoc);
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

std::unique_ptr<RdKafka::KafkaConsumer> KafkaConsumerOperator::createKafkaConsumer() const {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto setConf = [confPtr = conf.get()](const std::string& confName, auto confValue) {
        std::string errstr;
        if (confPtr->set(confName, confValue, errstr) != RdKafka::Conf::CONF_OK) {
            uasserted(ErrorCodes::UnknownError,
                      str::stream() << "KafkaConsumerOperator failed while setting configuration "
                                    << confName << " with error: " << errstr);
        }
    };
    setConf("bootstrap.servers", _options.bootstrapServers);
    setConf("log.connection.close", "false");
    setConf("topic.metadata.refresh.interval.ms", "-1");
    setConf("enable.auto.commit", "false");
    setConf("enable.auto.offset.store", "false");
    setConf("group.id", _options.consumerGroupId);
    for (const auto& config : _options.authConfig) {
        setConf(config.first, config.second);
    }

    std::string err;
    std::unique_ptr<RdKafka::KafkaConsumer> kafkaConsumer(
        RdKafka::KafkaConsumer::create(conf.get(), err));
    uassert(ErrorCodes::UnknownError,
            str::stream() << "KafkaConsumerOperator failed to create kafka consumer with error: "
                          << err,
            kafkaConsumer);

    return kafkaConsumer;
}

std::vector<int64_t> KafkaConsumerOperator::getCommittedOffsets() const {
    std::vector<std::unique_ptr<RdKafka::TopicPartition>> partitionsHolder;
    std::vector<RdKafka::TopicPartition*> partitions;

    // Number of partitions should have been fetched before fetching the committed offsets.
    invariant(_numPartitions);
    partitionsHolder.reserve(*_numPartitions);
    partitions.reserve(*_numPartitions);
    for (int32_t partition = 0; partition < *_numPartitions; ++partition) {
        std::unique_ptr<RdKafka::TopicPartition> p;
        p.reset(RdKafka::TopicPartition::create(_options.topicName, partition));
        partitions.push_back(p.get());
        partitionsHolder.push_back(std::move(p));
    }

    if (_options.isTest) {
        return {};
    }

    auto kafkaConsumer = createKafkaConsumer();
    RdKafka::ErrorCode errCode =
        kafkaConsumer->committed(partitions, _options.kafkaRequestTimeoutMs.count());
    tassert(
        8385400,
        str::stream() << "KafkaConsumerOperator failed to get committed offsets with error code: "
                      << errCode,
        errCode == RdKafka::ERR_NO_ERROR);
    tassert(8385401,
            "KafkaConsumerOperator unexpected number of partitions received from the topic",
            int64_t(partitions.size()) == *_numPartitions);

    bool hasValidOffsets{false};
    std::vector<int64_t> committedOffsets;
    committedOffsets.resize(partitions.size());
    for (const auto& partition : partitions) {
        if (partition->offset() != RdKafka::Topic::OFFSET_INVALID) {
            committedOffsets[partition->partition()] = partition->offset();
            hasValidOffsets = true;
        } else {
            // Ensure that either all partitions have a committed offset or all of them don't
            // have a committed offset, there shouldn't be a mix bag.
            tassert(8385402,
                    "KafkaConsumerOperator subset of partitions have a committed offset and a "
                    "subset do not have a committed offset",
                    !hasValidOffsets);
        }
    }

    if (hasValidOffsets) {
        return committedOffsets;
    }

    return {};
}

void KafkaConsumerOperator::commitOffsets(CheckpointId checkpointId) {
    // Checkpoints should always be committed in the order that they were added to
    // the `_uncommittedCheckpoints` queue.
    auto [id, checkpointState] = std::move(_uncommittedCheckpoints.front());
    _uncommittedCheckpoints.pop();
    tassert(7799700,
            "KafkaConsumerOperator received request to commit offsets for unknown or out of order "
            "checkpoint ID",
            id == checkpointId);
    if (_options.isTest) {
        return;
    }

    const auto& partitions = checkpointState.getPartitions();
    std::vector<std::unique_ptr<RdKafka::TopicPartition>> topicPartitionsHolder;
    std::vector<RdKafka::TopicPartition*> topicPartitions;
    topicPartitionsHolder.reserve(partitions.size());
    topicPartitions.reserve(partitions.size());

    invariant(partitions.size() == _consumers.size());
    for (const auto& partitionState : partitions) {
        std::unique_ptr<RdKafka::TopicPartition> tp;
        tp.reset(
            RdKafka::TopicPartition::create(_options.topicName, partitionState.getPartition()));
        tp->set_offset(partitionState.getOffset());
        topicPartitions.push_back(tp.get());
        topicPartitionsHolder.push_back(std::move(tp));
    }

    auto kafkaConsumer = createKafkaConsumer();
    RdKafka::ErrorCode errCode = kafkaConsumer->commitSync(topicPartitions);
    uassert(ErrorCodes::UnknownError,
            str::stream() << "KafkaConsumerOperator failed to commit offsets with error code: "
                          << errCode,
            errCode == RdKafka::ERR_NO_ERROR);

    // After committing offsets to the kafka broker, update the consumer info on what the
    // latest committed offsets are.
    for (size_t p = 0; p < _consumers.size(); ++p) {
        auto& consumerInfo = _consumers[p];
        const auto* committedPartition = topicPartitions[p];
        consumerInfo.checkpointOffset = committedPartition->offset();
    }
}

OperatorStats KafkaConsumerOperator::doGetStats() {
    OperatorStats stats{_stats};
    for (auto& consumerInfo : _consumers) {
        stats += consumerInfo.consumer->getStats();
    }
    return stats;
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
    options.startOffset = startOffset;
    options.authConfig = _options.authConfig;
    options.kafkaRequestTimeoutMs = _options.kafkaRequestTimeoutMs;
    options.kafkaRequestFailureSleepDurationMs = _options.kafkaRequestFailureSleepDurationMs;
    if (_options.isTest) {
        consumerInfo.consumer = std::make_unique<FakeKafkaPartitionConsumer>(std::move(options));
    } else {
        consumerInfo.consumer =
            std::make_unique<KafkaPartitionConsumer>(_context, std::move(options));
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

    KafkaSourceCheckpointState state;
    state.setPartitions(std::move(partitions));
    state.setConsumerGroupId(StringData(_options.consumerGroupId));
    _uncommittedCheckpoints.push({controlMsg.checkpointMsg->id, state});

    LOGV2_INFO(77177,
               "KafkaConsumerOperator adding state to checkpoint",
               "state"_attr = state.toBSON(),
               "context"_attr = _context,
               "checkpointId"_attr = controlMsg.checkpointMsg->id);
    if (_context->oldCheckpointStorage) {
        _context->oldCheckpointStorage->addState(controlMsg.checkpointMsg->id,
                                                 _operatorId,
                                                 std::move(state).toBSON(),
                                                 0 /* chunkNumber */);
    } else {
        invariant(_context->checkpointStorage);
        auto writer = _context->checkpointStorage->createStateWriter(controlMsg.checkpointMsg->id,
                                                                     _operatorId);
        _context->checkpointStorage->appendRecord(writer.get(), Document{state.toBSON()});
    }
}

std::vector<KafkaConsumerPartitionState> KafkaConsumerOperator::getPartitionStates() const {
    std::vector<KafkaConsumerPartitionState> states;
    states.reserve(_consumers.size());

    for (const auto& consumerInfo : _consumers) {
        int64_t currentOffset{0};
        if (consumerInfo.maxOffset) {
            currentOffset = *consumerInfo.maxOffset + 1;
        } else if (auto consumerStartOffset = consumerInfo.consumer->getStartOffset()) {
            // `startOffset` is only set after the kafka connection has been
            // established, otherwise this will return an empty value.
            currentOffset = *consumerStartOffset;
        }

        states.push_back(
            KafkaConsumerPartitionState{.partition = consumerInfo.partition,
                                        .currentOffset = currentOffset,
                                        .checkpointOffset = consumerInfo.checkpointOffset});
    }

    return states;
}

}  // namespace streams
