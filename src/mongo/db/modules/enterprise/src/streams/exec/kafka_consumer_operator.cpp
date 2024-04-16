/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/kafka_consumer_operator.h"

#include <chrono>
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
#include "streams/exec/stream_stats.h"
#include "streams/exec/util.h"
#include "streams/exec/watermark_combiner.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

KafkaConsumerOperator::Connector::Connector(Context* context, Options options)
    : _context(context), _options(std::move(options)) {}

KafkaConsumerOperator::Connector::~Connector() {
    stop();
}

void KafkaConsumerOperator::Connector::start() {
    _options.consumer->init();
    _options.consumer->start();

    invariant(!_connectionThread.joinable());
    _connectionThread = stdx::thread{[this]() { connectLoop(); }};
}

void KafkaConsumerOperator::Connector::stop() {
    // Stop the connection thread.
    bool joinThread{false};
    if (_connectionThread.joinable()) {
        stdx::lock_guard<Latch> lock(_mutex);
        _shutdown = true;
        joinThread = true;
    }
    if (joinThread) {
        // Wait for the connection thread to exit.
        _connectionThread.join();
    }

    _options.consumer->stop();
}

ConnectionStatus KafkaConsumerOperator::Connector::getConnectionStatus() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _connectionStatus;
}

void KafkaConsumerOperator::Connector::setConnectionStatus(ConnectionStatus status) {
    stdx::unique_lock lock(_mutex);
    _connectionStatus = status;
}

boost::optional<int64_t> KafkaConsumerOperator::Connector::getNumPartitions() {
    stdx::lock_guard<Latch> lock(_mutex);
    return _numPartitions;
}

void KafkaConsumerOperator::Connector::connectLoop() {
    invariant(!_numPartitions);

    try {
        boost::optional<int64_t> numPartitions;
        while (true) {
            auto connectionStatus = _options.consumer->getConnectionStatus();
            if (connectionStatus.isConnected()) {
                numPartitions = _options.consumer->getNumPartitions();
                if (numPartitions) {
                    LOGV2_INFO(74703,
                               "Retrieved topic partition count",
                               "topicName"_attr = _options.topicName,
                               "context"_attr = _context,
                               "numPartitions"_attr = *numPartitions);
                    invariant(*numPartitions > 0);
                }
            }

            {
                stdx::lock_guard<Latch> lock(_mutex);
                _connectionStatus = connectionStatus;
                if (numPartitions) {
                    _numPartitions = numPartitions;
                }

                if (_shutdown || _numPartitions || connectionStatus.isError()) {
                    break;
                }
            }

            // Sleep for a bit before calling getNumPartitions() again.
            stdx::this_thread::sleep_for(
                stdx::chrono::milliseconds(_options.kafkaRequestFailureSleepDurationMs));
        }
    } catch (const std::exception& e) {
        LOGV2_ERROR(8155001,
                    "Unexpected exception while connecting to kafka $source",
                    "exception"_attr = e.what());
        setConnectionStatus(ConnectionStatus{
            ConnectionStatus::kError,
            {{ErrorCodes::Error{8155002}, "$source encountered unkown error while connecting."},
             e.what()}});
    }
}

KafkaConsumerOperator::KafkaConsumerOperator(Context* context, Options options)
    : SourceOperator(context, /*numOutputs*/ 1), _options(std::move(options)) {
    if (_options.testOnlyNumPartitions) {
        invariant(_options.isTest);
        _numPartitions = _options.testOnlyNumPartitions;
    }
}

void KafkaConsumerOperator::doStart() {
    if (_context->restoreCheckpointId) {
        // De-serialize and verify the state.
        boost::optional<mongo::BSONObj> bsonState;
        invariant(_context->checkpointStorage);
        auto reader = _context->checkpointStorage->createStateReader(*_context->restoreCheckpointId,
                                                                     _operatorId);
        auto record = _context->checkpointStorage->getNextRecord(reader.get());
        CHECKPOINT_RECOVERY_ASSERT(
            *_context->restoreCheckpointId, _operatorId, "state should exist", record);
        bsonState = record->toBson();

        CHECKPOINT_RECOVERY_ASSERT(
            *_context->restoreCheckpointId, _operatorId, "state chunk 0 should exist", bsonState);
        _restoredCheckpointState = KafkaSourceCheckpointState::parseOwned(
            IDLParserContext(getName()), std::move(*bsonState));
    }

    invariant(_consumers.empty());
    // We need to wait until a connection is established with the input source before we can start
    // the per-partition consumer instances.

    if (_numPartitions) {
        // We already know the topic partition count. This is only true on test-only code path.
        init();
    } else {
        // Now create a Connector instace.
        Connector::Options options;
        options.topicName = _options.topicName;
        options.kafkaRequestFailureSleepDurationMs = _options.kafkaRequestFailureSleepDurationMs;
        // Create a KafkaPartitionConsumer instance just to determine the partition count for the
        // topic. Note that we will destroy this instance soon and won't be using it for reading
        // any input documents.
        options.consumer = createKafkaPartitionConsumer(/*partition*/ 0, _options.startOffset);
        _connector = std::make_unique<Connector>(_context, std::move(options));
        _connector->start();
    }
}

void KafkaConsumerOperator::doStop() {
    if (_connector) {
        _connector->stop();
        _connector.reset();
    }

    // Stop all partition consumers.
    for (auto& consumerInfo : _consumers) {
        consumerInfo.consumer->stop();
    }
    _consumers.clear();

    bool joinThread{false};
    if (_groupConsumerThread.joinable()) {
        stdx::unique_lock lock(_groupConsumerMutex);
        _groupConsumerThreadShutdown = true;
        _groupConsumerThreadCond.notify_one();
        joinThread = true;
    }
    if (joinThread) {
        _groupConsumerThread.join();
    }

    if (_groupConsumer) {
        // Shut down the RdKafka::KafkaConsumer.
        auto errorCode = _groupConsumer->unsubscribe();
        if (errorCode != RdKafka::ERR_NO_ERROR) {
            LOGV2_WARNING(8674600,
                          "Error while unsubscribing from kafka",
                          "errorCode"_attr = errorCode,
                          "errorMsg"_attr = RdKafka::err2str(errorCode),
                          "context"_attr = _context);
        }
    }
}

void KafkaConsumerOperator::initFromCheckpoint() {
    invariant(_numPartitions);
    invariant(_consumers.empty());
    invariant(_restoredCheckpointState);

    LOGV2_INFO(77187,
               "KafkaConsumerOperator restoring from checkpoint",
               "state"_attr = _restoredCheckpointState->toBSON(),
               "checkpointId"_attr = *_context->restoreCheckpointId);

    const auto& partitions = _restoredCheckpointState->getPartitions();
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
    if (auto consumerGroupId = _restoredCheckpointState->getConsumerGroupId(); consumerGroupId) {
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

void KafkaConsumerOperator::groupConsumerBackgroundLoop() {
    bool shutdown{false};
    std::vector<RdKafka::TopicPartition*> assignedPartitions;
    while (!shutdown) {
        stdx::unique_lock lock(_groupConsumerMutex);
        // TODO(SERVER-87007): Consider promoting the dasserts in this routine to errors that stop
        // processing. Currently we don't want errors in this routine to fail processing in
        // production.

        // Get the assigned partitions.
        std::vector<RdKafka::TopicPartition*> partitions;
        auto err = _groupConsumer->assignment(partitions);
        if (err != RdKafka::ERR_NO_ERROR) {
            dassert(false);
            LOGV2_WARNING(8674610,
                          "Error from librdkafka assignment call",
                          "err"_attr = int(err),
                          "errMsg"_attr = RdKafka::err2str(err));
        }

        if (err == RdKafka::ERR_NO_ERROR && partitions != assignedPartitions) {
            // If we've been assigned new partitions, call pause on all of them.
            assignedPartitions = std::move(partitions);
            auto err = _groupConsumer->pause(assignedPartitions);
            if (err == RdKafka::ERR_NO_ERROR) {
                for (const auto& partition : assignedPartitions) {
                    if (partition->err() != RdKafka::ERR_NO_ERROR) {
                        dassert(false);
                        LOGV2_WARNING(8674607,
                                      "Error from librdkafka pause call for topic",
                                      "err"_attr = int(err),
                                      "errMsg"_attr = RdKafka::err2str(err),
                                      "topic"_attr = partition->topic(),
                                      "partition"_attr = partition->partition());
                    }
                }
            } else {
                dassert(false);
                LOGV2_WARNING(8674609,
                              "Error from librdkafka pause call",
                              "err"_attr = int(err),
                              "errMsg"_attr = RdKafka::err2str(err));
            }
        }

        // Even though we don't use _groupConsumer to read data messages, we do have to call
        // consume, from the RdKafka::KafkaConsumer::consume documentation:
        //    "An application should make sure to call consume() at regular
        //    intervals, even if no messages are expected, to serve any
        //    queued callbacks waiting to be called."
        // We use a short timeout because we don't expect to see any messages and don't want to
        // block in the consumer call.
        std::unique_ptr<RdKafka::Message> msg{_groupConsumer->consume(/* timeout_ms */ 10)};
        // We allow ERR__TIMED_OUT and ERR__MAX_POLL_EXCEEDED because those are expected errors from
        // librdkafka when there are no messages to read.
        if (msg != nullptr && msg->err() != RdKafka::ERR__TIMED_OUT &&
            msg->err() != RdKafka::ERR__MAX_POLL_EXCEEDED) {
            dassert(false);
            LOGV2_WARNING(8674611,
                          "Unexpected data msg in groupConsumerBackgroundTask",
                          "partition"_attr = msg->partition(),
                          "offset"_attr = msg->offset(),
                          "len"_attr = msg->len(),
                          "err"_attr = msg->err());
        }

        // Sleep for 1 minute or shutdown.
        shutdown = _groupConsumerThreadCond.wait_for(
            lock, std::chrono::minutes(1), [this]() { return _groupConsumerThreadShutdown; });
    }
}

void KafkaConsumerOperator::init() {
    invariant(_numPartitions);
    invariant(_consumers.empty());

    if (_options.useWatermarks) {
        invariant(!_watermarkCombiner);
        _watermarkCombiner = std::make_unique<WatermarkCombiner>(*_numPartitions);
    }

    if (!_options.isTest) {
        // _groupConsumer is not used to actually read messages.
        // It's used only for retrieving and committing offsets for a Kafka consumer group.
        _groupConsumer = createKafkaConsumer();
        // We call subscribe so our consumer reports itself as an active member of the group.
        auto errorCode = _groupConsumer->subscribe({_options.topicName});
        uassert(8674606,
                fmt::format("Subscribing to Kafka failed with {}: {}",
                            errorCode,
                            RdKafka::err2str(errorCode)),
                errorCode == RdKafka::ERR_NO_ERROR);

        // We run a background thread that ocassionally calls the rdkafka consume function.
        _groupConsumerThread = stdx::thread([this]() {
            try {
                groupConsumerBackgroundLoop();
            } catch (std::exception& e) {
                // TODO(SERVER-87007): Consider promoting this warning to an error.
                LOGV2_WARNING(8674608,
                              "Unexpected exception in groupConsumerBackgroundTask",
                              "exception"_attr = e.what());
            }
        });
    }

    if (_context->restoreCheckpointId) {
        initFromCheckpoint();
    } else {
        initFromOptions();
    }
    invariant(!_consumers.empty());
}

ConnectionStatus KafkaConsumerOperator::doGetConnectionStatus() {
    if (!_numPartitions) {
        invariant(_consumers.empty());
        tassert(ErrorCodes::InternalError, "Expected connector to be set.", _connector);
        auto connectionStatus = _connector->getConnectionStatus();
        if (connectionStatus.isConnected()) {
            // Initialize the state if the connection has been successfully established.
            _numPartitions = _connector->getNumPartitions();
            tassert(ErrorCodes::InternalError,
                    "Expected _numPartitions to be set",
                    _numPartitions && *_numPartitions > 0);
            _connector->stop();
            _connector.reset();

            // We successfully fetched the topic partition count.
            init();
        } else {
            return connectionStatus;
        }
    }
    invariant(!_consumers.empty());

    // Check if all the consumers are connected.
    tassert(ErrorCodes::InternalError,
            "Expected _numPartitions",
            _numPartitions && *_numPartitions > 0);
    tassert(ErrorCodes::InternalError,
            "Expected consumers.size() equal to _numPartitions",
            _consumers.size() == size_t(*_numPartitions));
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
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(sourceDoc));
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
        auto currStreamMeta = _context->streamMetaFieldName
            ? Value(bsonDoc.getField(*_context->streamMetaFieldName))
            : Value();
        sourceDoc.doc = boost::none;
        BSONObjBuilder objBuilder(std::move(bsonDoc));
        objBuilder.appendDate(_options.timestampOutputFieldName, eventTimestamp);
        StreamMetaSource streamMetaSource;
        streamMetaSource.setType(StreamMetaSourceTypeEnum::Kafka);
        streamMetaSource.setPartition(sourceDoc.partition);
        streamMetaSource.setOffset(sourceDoc.offset);
        streamMetaSource.setKey(mongo::ConstDataRange(std::move(sourceDoc.key)));
        streamMetaSource.setHeaders(std::move(sourceDoc.headers));
        StreamMeta streamMeta;
        streamMeta.setSource(std::move(streamMetaSource));
        if (_context->shouldProjectStreamMetaPriorToSinkStage()) {
            auto newStreamMeta = updateStreamMeta(currStreamMeta, streamMeta);
            objBuilder.append(*_context->streamMetaFieldName, newStreamMeta.toBson());
        }

        streamDoc = StreamDocument(Document(objBuilder.obj()));
        streamDoc->streamMeta = std::move(streamMeta);
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
    StreamMetaSource streamMetaSource;
    streamMetaSource.setType(StreamMetaSourceTypeEnum::Kafka);
    streamMetaSource.setPartition(sourceDoc.partition);
    streamMetaSource.setOffset(sourceDoc.offset);
    streamMetaSource.setKey(mongo::ConstDataRange(std::move(sourceDoc.key)));
    streamMetaSource.setHeaders(std::move(sourceDoc.headers));
    StreamMeta streamMeta;
    streamMeta.setSource(std::move(streamMetaSource));
    BSONObjBuilder objBuilder = streams::toDeadLetterQueueMsg(
        _context->streamMetaFieldName, streamMeta, std::move(sourceDoc.error));
    if (sourceDoc.doc) {
        objBuilder.append("doc", std::move(*sourceDoc.doc));
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
            uasserted(8720700,
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
    setConf("queued.max.messages.kbytes", "5000");

    for (const auto& config : _options.authConfig) {
        setConf(config.first, config.second);
    }

    std::string err;
    std::unique_ptr<RdKafka::KafkaConsumer> kafkaConsumer(
        RdKafka::KafkaConsumer::create(conf.get(), err));
    uassert(8720701,
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

    tassert(8674601, "Expected _groupConsumer to be set", _groupConsumer);
    RdKafka::ErrorCode errCode =
        _groupConsumer->committed(partitions, _options.kafkaRequestTimeoutMs.count());
    uassert(
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

BSONObj KafkaConsumerOperator::doOnCheckpointFlush(CheckpointId checkpointId) {
    if (_options.isTest) {
        return BSONObj{};
    }

    auto stateBson = _unflushedStateContainer.pop(checkpointId);
    auto checkpointState = KafkaSourceCheckpointState::parseOwned(
        IDLParserContext{"KafkaConsumerOperator::doOnCheckpointFlush"}, std::move(stateBson));

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

    if (_groupConsumer) {
        RdKafka::ErrorCode errCode = _groupConsumer->commitAsync(topicPartitions);
        uassert(8674605,
                str::stream() << "KafkaConsumerOperator failed to commit offsets with error code: "
                              << errCode << " and msg: " << RdKafka::err2str(errCode),
                errCode == RdKafka::ERR_NO_ERROR);
    }

    // After committing offsets to the kafka broker, update the consumer info on what the
    // latest committed offsets are.
    for (size_t p = 0; p < _consumers.size(); ++p) {
        auto& consumerInfo = _consumers[p];
        const auto* committedPartition = topicPartitions[p];
        consumerInfo.checkpointOffset = committedPartition->offset();
    }

    _lastCommittedCheckpointState = std::move(checkpointState);
    return _lastCommittedCheckpointState->toBSON();
}

OperatorStats KafkaConsumerOperator::doGetStats() {
    OperatorStats stats{_stats};
    for (auto& consumerInfo : _consumers) {
        stats += consumerInfo.consumer->getStats();
    }
    return stats;
}

void KafkaConsumerOperator::registerMetrics(MetricManager* metricManager) {
    _queueSizeGauge = metricManager->registerIntGauge(
        "source_operator_queue_size",
        /* description */ "Total docs currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context));
    _queueByteSizeGauge = metricManager->registerIntGauge(
        "source_operator_queue_bytesize",
        /* description */ "Total bytes currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context));
}

std::unique_ptr<KafkaPartitionConsumerBase> KafkaConsumerOperator::createKafkaPartitionConsumer(
    int32_t partition, int64_t startOffset) {
    KafkaPartitionConsumerBase::Options options;
    options.bootstrapServers = _options.bootstrapServers;
    options.topicName = _options.topicName;
    options.partition = partition;
    options.deserializer = _options.deserializer;
    options.maxNumDocsToReturn = _options.maxNumDocsToReturn;
    options.startOffset = startOffset;
    options.authConfig = _options.authConfig;
    options.kafkaRequestTimeoutMs = _options.kafkaRequestTimeoutMs;
    options.kafkaRequestFailureSleepDurationMs = _options.kafkaRequestFailureSleepDurationMs;
    options.queueSizeGauge = _queueSizeGauge;
    options.queueByteSizeGauge = _queueByteSizeGauge;
    options.enableKeysAndHeaders = _options.enableKeysAndHeaders;
    if (_options.isTest) {
        return std::make_unique<FakeKafkaPartitionConsumer>(std::move(options));
    } else {
        return std::make_unique<KafkaPartitionConsumer>(_context, std::move(options));
    }
}

KafkaConsumerOperator::ConsumerInfo KafkaConsumerOperator::createPartitionConsumer(
    int32_t partition, int64_t startOffset) {
    ConsumerInfo consumerInfo;
    consumerInfo.partition = partition;
    consumerInfo.consumer = createKafkaPartitionConsumer(partition, startOffset);
    return consumerInfo;
}

void KafkaConsumerOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    invariant(controlMsg.checkpointMsg && !controlMsg.watermarkMsg);
    processCheckpointMsg(controlMsg);
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

void KafkaConsumerOperator::processCheckpointMsg(const StreamControlMsg& controlMsg) {
    tassert(8155003, "Expecting checkpointMsg", controlMsg.checkpointMsg);
    tassert(8155004, "_consumers is empty", !_consumers.empty());
    std::vector<KafkaPartitionCheckpointState> partitions;
    partitions.reserve(_consumers.size());
    for (const ConsumerInfo& consumerInfo : _consumers) {
        int64_t checkpointStartingOffset{0};
        if (consumerInfo.maxOffset) {
            checkpointStartingOffset = *consumerInfo.maxOffset + 1;
        } else {
            auto consumerStartOffset = consumerInfo.consumer->getStartOffset();
            tassert(8155005, "consumerStartOffset is uninitialized", consumerStartOffset);
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
    _unflushedStateContainer.add(controlMsg.checkpointMsg->id, state.toBSON());
    LOGV2_INFO(77177,
               "KafkaConsumerOperator adding state to checkpoint",
               "state"_attr = state.toBSON(),
               "context"_attr = _context,
               "checkpointId"_attr = controlMsg.checkpointMsg->id);
    tassert(8155006, "checkpointStorage is uninitialized", _context->checkpointStorage);
    auto writer =
        _context->checkpointStorage->createStateWriter(controlMsg.checkpointMsg->id, _operatorId);
    _context->checkpointStorage->appendRecord(writer.get(), Document{state.toBSON()});
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

boost::optional<mongo::BSONObj> KafkaConsumerOperator::doGetRestoredState() {
    if (!_restoredCheckpointState) {
        return boost::none;
    }

    return _restoredCheckpointState->toBSON();
}

boost::optional<mongo::BSONObj> KafkaConsumerOperator::doGetLastCommittedState() {
    if (!_lastCommittedCheckpointState) {
        return boost::none;
    }

    return _lastCommittedCheckpointState->toBSON();
}

}  // namespace streams
