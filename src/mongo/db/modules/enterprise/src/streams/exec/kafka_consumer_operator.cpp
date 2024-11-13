/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/kafka_consumer_operator.h"

#include <chrono>
#include <fmt/format.h>
#include <rdkafka.h>
#include <rdkafkacpp.h>

#include "mongo/db/json.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/text.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/fake_kafka_partition_consumer.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/kafka_utils.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/util.h"
#include "streams/exec/watermark_combiner.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace {
MONGO_FAIL_POINT_DEFINE(slowKafkaSource);

bool topicPartitionExists(const std::vector<KafkaConsumerOperator::TopicPartition>& topicPartitions,
                          const std::string& topicName,
                          int32_t partitionId) {

    for (const auto& [topic, partition] : topicPartitions) {
        if (topicName == topic && partitionId == partition) {
            return true;
        }
    }
    return false;
}

using namespace mongo;

// Helper method used to create KafkaConsumer. Used from KafkaConsumerOperator::Connector
// and from KafkaConsumerOperator.
std::unique_ptr<RdKafka::KafkaConsumer> createKafkaConsumer(
    std::string bootstrapServers,
    std::string consumerGroupId,
    bool shouldEnableAutoCommit,
    mongo::stdx::unordered_map<std::string, std::string> authConfig,
    RdKafka::ResolveCb* resolveCb,
    RdKafka::ConnectCb* connectCb,
    RdKafka::EventCb* eventCb) {

    // conf will go out of scope at the end of this function but it should be fine since it is
    // expected to be valid only when it is being used in the call to RdKafkaConsumer::create
    // towards the end of this function.
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto setConf = [confPtr = conf.get()](const std::string& confName, auto confValue) {
        std::string errstr;
        if (confPtr->set(confName, confValue, errstr) != RdKafka::Conf::CONF_OK) {
            uasserted(8720700,
                      str::stream() << "KafkaConsumerOperator failed while setting configuration "
                                    << confName << " with error: " << errstr);
        }
    };
    setConf("bootstrap.servers", bootstrapServers);
    if (streams::isConfluentBroker(bootstrapServers)) {
        setConf("client.id", std::string(streams::kKafkaClientID));
    }
    setConf("log.connection.close", "false");
    setConf("topic.metadata.refresh.interval.ms", "-1");

    setConf("enable.auto.commit", "false");
    setConf("group.id", consumerGroupId);

    if (shouldEnableAutoCommit) {
        setConf("enable.auto.commit", "true");
        setConf("auto.commit.interval.ms", "500");
    }

    setConf("enable.auto.offset.store", "false");
    setConf("queued.max.messages.kbytes", "5000");

    if (resolveCb) {
        setConf("resolve_cb", resolveCb);
        if (connectCb) {
            setConf("connect_cb", connectCb);
        }
    }

    if (eventCb) {
        setConf("event_cb", eventCb);
    }

    for (const auto& config : authConfig) {
        setConf(config.first, config.second);
    }

    // KafkaConsumer::create internally makes copies of any bits it needs from conf and so
    // we are ok with letting conf destruct after this call.
    std::string err;
    std::unique_ptr<RdKafka::KafkaConsumer> kafkaConsumer(
        RdKafka::KafkaConsumer::create(conf.get(), err));
    uassert(8720701,
            str::stream() << "KafkaConsumerOperator failed to create kafka consumer with error: "
                          << err,
            kafkaConsumer);

    return kafkaConsumer;
}

}  // namespace

KafkaConsumerOperator::TopicPartition::TopicPartition(std::string topicName, int32_t partition)
    : topic{std::move(topicName)}, partitionId{partition} {}

mongo::BSONObj KafkaConsumerOperator::TopicPartition::toBSON() const {
    return BSON("topic" << topic << "partition" << partitionId);
}

KafkaConsumerOperator::Connector::Connector(Context* context, Options options)
    : _context(context), _options(std::move(options)) {

    // Setup the resolve callback if so configured.
    if (_options.gwproxyEndpoint) {
        _resolveCbImpl =
            std::make_shared<KafkaResolveCallback>(_context,
                                                   "KafkaConsumerOperator::Connector",
                                                   *_options.gwproxyEndpoint) /* target proxy */;

        // Setup the connect callback if authentication is required.
        if (_options.gwproxyKey) {
            _connectCbImpl = std::make_shared<KafkaConnectAuthCallback>(
                _context,
                "KafkaConsumerOperator::Connector",
                *_options.gwproxyKey /* symmetricKey */,
                10 /* connection timeout unit:seconds */);
        }
    }

    // Setup event callback since we need to determine the details of connect errors
    _eventCallback =
        std::make_unique<KafkaEventCallback>(_context, "KafkaConsumerOperator::Connector");

    _consumer = streams::createKafkaConsumer(_options.bootstrapServers,
                                             _options.consumerGroupId,
                                             false, /* shouldEnableAutoCommit */
                                             _options.authConfig,
                                             _resolveCbImpl ? _resolveCbImpl.get() : nullptr,
                                             _connectCbImpl ? _connectCbImpl.get() : nullptr,
                                             _eventCallback.get());
}

KafkaConsumerOperator::Connector::~Connector() {
    stop();
}

void KafkaConsumerOperator::Connector::start() {
    invariant(!_connectionThread.joinable());
    invariant(_consumer);
    _connectionThread = stdx::thread{[this]() {
        try {
            retrieveTopicPartitions();
        } catch (const SPException& e) {
            onConnectionError(e.toStatus());
        } catch (const DBException& e) {
            onConnectionError(e.toStatus());
        } catch (const std::exception& e) {
            LOGV2_ERROR(8155001,
                        "Unexpected exception while connecting to kafka $source",
                        "context"_attr = _context,
                        "exception"_attr = e.what());
            onConnectionError(
                SPStatus{mongo::Status{ErrorCodes::InternalError,
                                       "Unexpected exception connecting to kafka $source."},
                         e.what()});
        }
    }};
}

void KafkaConsumerOperator::Connector::onConnectionError(SPStatus status) {
    status = _eventCallback->appendRecentErrorsToStatus(status);
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _connectionStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
}

void KafkaConsumerOperator::Connector::stop() {
    // Stop the connection thread.
    bool joinThread{false};
    if (_connectionThread.joinable()) {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _shutdown = true;
        joinThread = true;
    }
    if (joinThread) {
        // Wait for the connection thread to exit.
        _connectionThread.join();
    }
    if (_consumer) {
        // Shut down the RdKafka::KafkaConsumer.
        auto errorCode = _consumer->close();
        if (errorCode != RdKafka::ERR_NO_ERROR) {
            LOGV2_WARNING(8674612,
                          "Error while closing kafka consumer",
                          "errorCode"_attr = errorCode,
                          "errorMsg"_attr = RdKafka::err2str(errorCode),
                          "context"_attr = _context);
        }
    }
}

ConnectionStatus KafkaConsumerOperator::Connector::getConnectionStatus() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _connectionStatus;
}

void KafkaConsumerOperator::Connector::setConnectionStatus(ConnectionStatus status) {
    stdx::unique_lock lock(_mutex);
    _connectionStatus = status;
}

std::vector<KafkaConsumerOperator::TopicPartition>
KafkaConsumerOperator::Connector::getTopicPartitions() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _topicPartitions;
}

boost::optional<std::string> KafkaConsumerOperator::Connector::getVerboseCallbackErrorsIfExists() {
    // Any error that _resolveCbImpl returns will be fatal, so it's not
    // necessary to accumulate errors from _connectCbImpl if we error
    // in the resolver, we can return them immediately.
    if (_resolveCbImpl && _resolveCbImpl->hasErrors()) {
        return _resolveCbImpl->getAllErrorsAsString();
    }
    if (_connectCbImpl && _connectCbImpl->hasErrors()) {
        return _connectCbImpl->getAllErrorsAsString();
    }

    return boost::none;
}

void KafkaConsumerOperator::Connector::retrieveTopicPartitions() {
    invariant(getTopicPartitions().empty());
    std::vector<TopicPartition> topicPartitions;

    for (const auto& topicName : _options.topicNames) {
        std::string errStr;
        std::unique_ptr<RdKafka::Topic> rdTopic{
            RdKafka::Topic::create(_consumer.get(), topicName, nullptr, errStr)};
        if (!rdTopic) {
            if (errStr.empty()) {
                errStr = "Could not create topic while trying to connect to Kafka";
            }
            LOGV2_INFO(9358011,
                       "could not create topic",
                       "context"_attr = _context,
                       "topic"_attr = topicName,
                       "error"_attr = errStr);
            uasserted(ErrorCodes::StreamProcessorKafkaConnectionError, errStr);
        }

        RdKafka::Metadata* metadata{nullptr};
        RdKafka::ErrorCode resp =
            _consumer->metadata(false, rdTopic.get(), &metadata, kKafkaRequestTimeoutMs);
        std::unique_ptr<RdKafka::Metadata> metadataHolder{metadata};
        const boost::optional<std::string> verboseCallbackErrors =
            getVerboseCallbackErrorsIfExists();
        if (resp != RdKafka::ERR_NO_ERROR || !metadata || metadata->topics()->size() != 1 ||
            metadata->topics()->at(0)->topic() != topicName) {
            LOGV2_INFO(9358012,
                       "could not load topic metadata",
                       "context"_attr = _context,
                       "topic"_attr = topicName,
                       "errorCode"_attr = resp,
                       "errorMsg"_attr = RdKafka::err2str(resp));

            uassert(ErrorCodes::StreamProcessorKafkaConnectionError,
                    verboseCallbackErrors
                        ? verboseCallbackErrors.get()
                        : kafkaErrToString("Could not connect to the Kafka topic", resp),
                    resp == RdKafka::ERR_NO_ERROR);
        }

        auto* partitions = metadataHolder->topics()->at(0)->partitions();
        tassert(ErrorCodes::StreamProcessorKafkaConnectionError,
                "expected partitions to be non-null",
                partitions);
        if (partitions->empty()) {
            LOGV2_INFO(9358013,
                       "topic does not exist",
                       "context"_attr = _context,
                       "topic"_attr = topicName);
            uasserted(
                ErrorCodes::StreamProcessorKafkaConnectionError,
                fmt::format("no partitions found in topic {}. Does the topic exist?", topicName));
        }

        // Iterate over all partitions of topicName, check whether any of them is reporting an
        // error
        for (const auto& partition : *partitions) {
            auto partitionErr = partition->err();
            if (partitionErr != RdKafka::ERR_NO_ERROR) {
                LOGV2_INFO(9358014,
                           "partition has error:",
                           "context"_attr = _context,
                           "topic"_attr = topicName,
                           "partition"_attr = partition->id(),
                           "error"_attr = RdKafka::err2str(partitionErr));
                uasserted(ErrorCodes::StreamProcessorKafkaConnectionError,
                          fmt::format("topic[{}]/partition[{}] has error: [{}]",
                                      topicName,
                                      partition->id(),
                                      RdKafka::err2str(partition->err())));
            }

            // All good, add this topic/partition
            topicPartitions.emplace_back(topicName, partition->id());
        }
    }

    std::sort(topicPartitions.begin(), topicPartitions.end(), TopicPartitionCmp());
    LOGV2_INFO(9523301,
               "Found topic/partitions: ",
               "context"_attr = _context,
               "topicPartitions"_attr = topicPartitions);

    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _connectionStatus = ConnectionStatus{ConnectionStatus::Status::kConnected};
    _topicPartitions = std::move(topicPartitions);
}

KafkaConsumerOperator::KafkaConsumerOperator(Context* context, Options options)
    : SourceOperator(context, /*numOutputs*/ 1), _options(std::move(options)) {
    if (!_options.testOnlyTopicPartitions.empty()) {
        invariant(_options.isTest);
        _topicPartitions = _options.testOnlyTopicPartitions;
    }
    // We won't be using _sourceBufferHandle, so release it.
    _sourceBufferHandle.reset();
    _stats.connectionType = ConnectionTypeEnum::Kafka;
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
    // We need to wait until a connection is established with the input source before we can
    // start the per-partition consumer instances.

    if (!_topicPartitions.empty()) {
        // We already know the topic partition map. This is only true on test-only code path.
        init();
    } else {
        // Now create a Connector instace.
        Connector::Options options{
            .topicNames = _options.topicNames,
            .kafkaRequestFailureSleepDurationMs = _options.kafkaRequestFailureSleepDurationMs,
            .bootstrapServers = _options.bootstrapServers,
            .consumerGroupId = _options.consumerGroupId,
            .authConfig = _options.authConfig,
            .gwproxyEndpoint = _options.gwproxyEndpoint,
            .gwproxyKey = _options.gwproxyKey,
        };

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
            LOGV2_WARNING(8674613,
                          "Error while unsubscribing from kafka",
                          "errorCode"_attr = errorCode,
                          "errorMsg"_attr = RdKafka::err2str(errorCode),
                          "context"_attr = _context);
        }
    }
}

boost::optional<int32_t> KafkaConsumerOperator::getPartitionIdx(const std::string& topicName,
                                                                int32_t partitionId) {
    invariant(!_topicPartitions.empty());
    auto itr = std::find(
        _topicPartitions.begin(), _topicPartitions.end(), TopicPartition{topicName, partitionId});
    if (itr != _topicPartitions.end()) {
        return std::distance(_topicPartitions.begin(), itr);
    }
    return boost::none;
}

void KafkaConsumerOperator::initFromCheckpoint() {
    invariant(!_topicPartitions.empty());
    invariant(_consumers.empty());
    invariant(_restoredCheckpointState);

    LOGV2_INFO(77187,
               "KafkaConsumerOperator restoring from checkpoint",
               "context"_attr = _context,
               "state"_attr = _restoredCheckpointState->toBSON(),
               "checkpointId"_attr = *_context->restoreCheckpointId);

    size_t expectedNumTopicPartitions = _topicPartitions.size();
    const auto& partitions = _restoredCheckpointState->getPartitions();
    CHECKPOINT_RECOVERY_ASSERT(
        *_context->restoreCheckpointId,
        _operatorId,
        str::stream() << "partition count in the checkpoint (" << partitions.size() << ") "
                      << "does not match the partition count of Kafka topics ("
                      << expectedNumTopicPartitions << ")",
        partitions.size() == expectedNumTopicPartitions);

    // Use the consumer group ID from the checkpoint. The consumer group ID should never change
    // during the lifetime of a stream processor. The consumer group ID is optional in the
    // checkpoint data for backwards compatibility reasons and when the SP is ephemeral.
    if (auto consumerGroupId = _restoredCheckpointState->getConsumerGroupId(); consumerGroupId) {
        _options.consumerGroupId = std::string(*consumerGroupId);
    }

    // Create KafkaPartitionConsumer instances from the checkpoint data.
    auto numPartitions = partitions.size();
    _consumers.reserve(numPartitions);
    if (_options.enableAutoCommit) {
        _partitionOffsets.reserve(numPartitions);
        _partitionOffsetsHolder.reserve(numPartitions);
    }
    for (const auto& partitionState : partitions) {
        std::string chkptTopic;
        if (partitionState.getTopic()) {
            chkptTopic = std::string{*partitionState.getTopic()};
        } else {
            // If partitionState does not have the topic, then it means that the checkpoint was
            // taken from a version of the code that only supported on topic
            // TODO(SERVER-95131) Remove this check once all checkpoints are on v3 or above
            tassert(ErrorCodes::InternalError,
                    "expected only one topic in this kafka source",
                    _options.topicNames.size() == 1);
            chkptTopic = _options.topicNames.front();
        }
        int32_t chkptPartitionId = partitionState.getPartition();

        // make sure this topicPartition still exists in the cluster
        tassert(ErrorCodes::InternalError,
                "checkpoint topic partition not found in current cluster",
                topicPartitionExists(_topicPartitions, chkptTopic, chkptPartitionId));

        // Create the consumer with the offset in the checkpoint.
        ConsumerInfo consumerInfo = createPartitionConsumer(
            chkptTopic,
            chkptPartitionId,
            partitionState.getOffset(),
            getRdKafkaQueuedMaxMessagesKBytes(_context->featureFlags, partitions.size()));

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

            boost::optional<int32_t> inputIdx = getPartitionIdx(chkptTopic, chkptPartitionId);
            CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                       _operatorId,
                                       str::stream() << "Could not get inputIdx for topic/partition"
                                                     << chkptTopic << "/" << chkptPartitionId,
                                       inputIdx)

            consumerInfo.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
                *inputIdx, _watermarkCombiner.get(), watermark);
            consumerInfo.partitionIdleTimeoutMs = _options.partitionIdleTimeoutMs;
        }

        if (_options.enableAutoCommit) {
            std::unique_ptr<RdKafka::TopicPartition> tp{
                RdKafka::TopicPartition::create(consumerInfo.topic, consumerInfo.partition)};
            _partitionOffsets.push_back(tp.get());
            _partitionOffsetsHolder.push_back(std::move(tp));
        }

        consumerInfo.consumer->init();
        consumerInfo.consumer->start();
        _consumers.push_back(std::move(consumerInfo));
    }
}

void KafkaConsumerOperator::initFromOptions() {
    invariant(!_topicPartitions.empty());
    invariant(_consumers.empty());

    // Create KafkaPartitionConsumer instances, one for each topic partition.
    auto numTopicPartitions = _topicPartitions.size();
    _consumers.reserve(numTopicPartitions);
    if (_options.enableAutoCommit) {
        _partitionOffsets.reserve(numTopicPartitions);
        _partitionOffsetsHolder.reserve(numTopicPartitions);
    }
    auto committedOffsets = getCommittedOffsets();
    for (const auto& [topic, partitionId] : _topicPartitions) {
        int64_t startOffset = _options.startOffset;
        if (!committedOffsets.empty()) {
            auto itr = committedOffsets.find({topic, partitionId});
            tassert(ErrorCodes::InternalError,
                    "Expected to find committed offset for partition.",
                    itr != committedOffsets.end());
            startOffset = itr->second;
        }

        ConsumerInfo consumerInfo = createPartitionConsumer(
            topic,
            partitionId,
            startOffset,
            getRdKafkaQueuedMaxMessagesKBytes(_context->featureFlags, _topicPartitions.size()));
        if (_options.useWatermarks) {
            invariant(_watermarkCombiner);
            boost::optional<int32_t> inputIdx = getPartitionIdx(topic, partitionId);
            tassert(ErrorCodes::InternalError,
                    "Expected to find partitionIdx for topic/partition",
                    inputIdx);
            consumerInfo.watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
                *inputIdx /* inputIdx */, _watermarkCombiner.get());
            consumerInfo.partitionIdleTimeoutMs = _options.partitionIdleTimeoutMs;

            // Capturing the initial time during construction is useful in the context of
            // idleness detection as it provides a baseline for subsequent events to compare to.
            consumerInfo.lastEventReadTimestamp = stdx::chrono::steady_clock::now();
        }
        if (_options.enableAutoCommit) {
            std::unique_ptr<RdKafka::TopicPartition> tp{
                RdKafka::TopicPartition::create(consumerInfo.topic, consumerInfo.partition)};
            _partitionOffsets.push_back(tp.get());
            _partitionOffsetsHolder.push_back(std::move(tp));
        }

        consumerInfo.consumer->init();
        consumerInfo.consumer->start();
        _consumers.push_back(std::move(consumerInfo));
    }
}

void KafkaConsumerOperator::groupConsumerBackgroundLoop() {
    bool shutdown{false};
    std::vector<RdKafka::TopicPartition*> assignedPartitions;
    while (!shutdown) {
        stdx::unique_lock lock(_groupConsumerMutex);
        // TODO(SERVER-87007): Consider promoting the dasserts in this routine to errors that
        // stop processing. Currently we don't want errors in this routine to fail processing in
        // production.

        // Get the assigned partitions.
        std::vector<RdKafka::TopicPartition*> partitions;
        auto err = _groupConsumer->assignment(partitions);
        if (err != RdKafka::ERR_NO_ERROR) {
            dassert(false);
            LOGV2_WARNING(8674610,
                          "Error from librdkafka assignment call",
                          "context"_attr = _context,
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
                                      "context"_attr = _context,
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
                              "context"_attr = _context,
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
        // We allow ERR__TIMED_OUT and ERR__MAX_POLL_EXCEEDED because those are expected errors
        // from librdkafka when there are no messages to read.
        if (msg != nullptr && msg->err() != RdKafka::ERR__TIMED_OUT &&
            msg->err() != RdKafka::ERR__MAX_POLL_EXCEEDED) {
            dassert(false);
            LOGV2_WARNING(8674611,
                          "Unexpected data msg in groupConsumerBackgroundTask",
                          "context"_attr = _context,
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
    invariant(!_topicPartitions.empty());
    invariant(_consumers.empty());

    if (_options.useWatermarks) {
        invariant(!_watermarkCombiner);
        _watermarkCombiner =
            std::make_unique<WatermarkCombiner>((int32_t)(_topicPartitions.size()));
    }

    if (!_options.isTest) {
        // _groupConsumer is not used to actually read messages.
        // It's used only for retrieving and committing offsets for a Kafka consumer group.
        _groupConsumer = createKafkaConsumer();
        // We call subscribe so our consumer reports itself as an active member of the group.
        auto errorCode = _groupConsumer->subscribe(_options.topicNames);
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
                              "context"_attr = _context,
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
    if (_topicPartitions.empty()) {
        invariant(_consumers.empty());
        tassert(ErrorCodes::InternalError, "Expected connector to be set.", _connector);
        auto connectionStatus = _connector->getConnectionStatus();
        if (connectionStatus.isConnected()) {
            // Initialize the state if the connector has obtained it
            _topicPartitions = _connector->getTopicPartitions();
            tassert(ErrorCodes::InternalError,
                    "Expected non-empty _topicPartitions",
                    !_topicPartitions.empty());
            _connector->stop();
            _connector.reset();

            // We successfully fetched the topic partition counts.
            init();
        } else {
            return connectionStatus;
        }
    }
    invariant(!_consumers.empty());
    tassert(ErrorCodes::InternalError,
            "Expected non-empty _topicPartitions",
            !_topicPartitions.empty());

    tassert(ErrorCodes::InternalError,
            "Expected consumers.size() equal to numPartitions",
            _consumers.size() == _topicPartitions.size());

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

    auto dataMsgMaxDocSize = std::min(_testOnlyDataMsgMaxDocSize, kDataMsgMaxDocSize);

    int32_t curDataMsgByteSize{0};
    auto newStreamDataMsg = [&]() {
        StreamDataMsg dataMsg;
        dataMsg.docs.reserve(dataMsgMaxDocSize + _options.maxNumDocsToReturn);
        curDataMsgByteSize = 0;
        return dataMsg;
    };

    StreamDataMsg dataMsg = newStreamDataMsg();

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
        if (force || int32_t(dataMsg.docs.size()) >= dataMsgMaxDocSize ||
            curDataMsgByteSize >= kDataMsgMaxByteSize) {
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
                dataMsg = newStreamDataMsg();

                if (_groupConsumer && _options.enableAutoCommit) {
                    for (size_t i = 0; i < _consumers.size(); i++) {
                        int64_t offset{0};
                        if (_consumers[i].maxOffset) {
                            offset = *_consumers[i].maxOffset;
                        } else {
                            auto consumerStartOffset = _consumers[i].consumer->getStartOffset();
                            tassert(9446500,
                                    "consumerStartOffset is uninitialized",
                                    consumerStartOffset);
                            offset = *consumerStartOffset;
                        }

                        invariant(_partitionOffsets[i]);
                        _partitionOffsets[i]->set_offset(offset);
                    }
                    auto errCode = _groupConsumer->offsets_store(_partitionOffsets);
                    if (errCode != RdKafka::ERR_NO_ERROR) {
                        // TODO(SERVER-87997): Throw error here if this error happens after initial
                        // partition setup period.
                        // This error is expected for a brief period on
                        // startup because the partitions the _groupConsumer is subscribed to
                        // haven't been set up yet.
                        LOGV2_WARNING(9446501,
                                      "Encountered error while storing offsets",
                                      "context"_attr = _context,
                                      "error"_attr = errCode);
                    }
                }
            } else if (newControlMsg) {
                _lastControlMsg = *newControlMsg;
                // Note that we send newControlMsg only if it differs from _lastControlMsg.
                sendControlMsg(/*outputIdx*/ 0, std::move(*newControlMsg));
            }
            return true;
        }
        return false;
    };

    bool flushed{false};
    while (!flushed && !consumerq.empty()) {
        // Prioritize consumers with the lowest local watermark.
        int consumerIdx = consumerq.top();
        consumerq.pop();
        auto& consumerInfo = _consumers[consumerIdx];

        auto sourceDocs = consumerInfo.consumer->getDocuments();
        dassert(int32_t(sourceDocs.size()) <= _options.maxNumDocsToReturn);

        if (MONGO_unlikely(slowKafkaSource.shouldFail())) {
            sleepFor(Seconds{2});
        }

        // Handle idleness time out.
        if (consumerInfo.partitionIdleTimeoutMs != stdx::chrono::milliseconds(0)) {
            dassert(consumerInfo.watermarkGenerator);
            const auto currentTime = stdx::chrono::steady_clock::now();

            // Check for idleness if we didn't get any documents, otherwise, we record the
            // current time and set the current partition as active.
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
            numInputBytes += sourceDoc.messageSizeBytes;
            invariant(!consumerInfo.maxOffset || sourceDoc.offset > *consumerInfo.maxOffset);
            consumerInfo.maxOffset = sourceDoc.offset;
            auto streamDoc =
                processSourceDocument(std::move(sourceDoc), consumerInfo.watermarkGenerator.get());
            if (streamDoc) {
                if (consumerInfo.watermarkGenerator) {
                    consumerInfo.watermarkGenerator->onEvent(streamDoc->minEventTimestampMs);
                }

                curDataMsgByteSize += streamDoc->doc.getApproximateSize();
                dataMsg.docs.push_back(std::move(*streamDoc));
            } else {
                // Invalid or late document, inserted into the dead letter queue.
                ++numDlqDocs;
            }
        }

        if (numInputDocs > 0) {
            // Since the partition produced some documents, add it back into the `consumerq`
            // priority queue.
            consumerq.push(consumerIdx);
        }

        incOperatorStats(OperatorStats{.numInputDocs = numInputDocs,
                                       .numInputBytes = numInputBytes,
                                       .numDlqDocs = numDlqDocs});
        if (_watermarkCombiner) {
            _stats.watermark = _watermarkCombiner->getCombinedWatermarkMsg().eventTimeWatermarkMs;
        }

        flushed = maybeFlush(/*force*/ false);
    }

    if (!flushed) {
        maybeFlush(/*force*/ true);
    }

    return totalNumInputDocs;
}

std::variant<std::vector<std::uint8_t>,
             std::string,
             mongo::BSONObj,
             std::int32_t,
             std::int64_t,
             double>
KafkaConsumerOperator::deserializeKafkaKey(std::vector<std::uint8_t> key,
                                           KafkaKeyFormatEnum keyFormat) {
    switch (keyFormat) {
        case KafkaKeyFormatEnum::BinData: {
            return key;
        }
        case KafkaKeyFormatEnum::String: {
            auto keyStr = StringData{reinterpret_cast<const char*>(key.data()),
                                     static_cast<size_t>(key.size())};
            if (!isValidUTF8(keyStr)) {
                return key;
            }
            std::string deserializedKey(key.begin(), key.end());
            return deserializedKey;
        }
        case KafkaKeyFormatEnum::Json: {
            auto keyStr = StringData{reinterpret_cast<const char*>(key.data()),
                                     static_cast<size_t>(key.size())};
            try {
                BSONObj deserializedKey = fromjson(keyStr);
                return deserializedKey;
            } catch (std::exception&) {
                return key;
            }
        }
        case KafkaKeyFormatEnum::Int: {
            if (key.size() != sizeof(int32_t)) {
                return key;
            }
            // Big-endian deserialization
            return ConstDataView(reinterpret_cast<char*>(key.data())).read<BigEndian<int32_t>>();
        }
        case KafkaKeyFormatEnum::Long: {
            if (key.size() != sizeof(int64_t)) {
                return key;
            }
            // Big-endian deserialization
            return ConstDataView(reinterpret_cast<char*>(key.data())).read<BigEndian<int64_t>>();
        }
        case KafkaKeyFormatEnum::Double: {
            if (key.size() != sizeof(double)) {
                return key;
            }
            // Big-endian deserialization
            return ConstDataView(reinterpret_cast<char*>(key.data())).read<BigEndian<double>>();
        }
        default: {
            MONGO_UNREACHABLE;
        }
    }
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
        streamMetaSource.setTopic(sourceDoc.topic);
        streamMetaSource.setPartition(sourceDoc.partition);
        streamMetaSource.setOffset(sourceDoc.offset);
        auto deserializedKey = deserializeKafkaKey(std::move(sourceDoc.key), _options.keyFormat);
        // If the required format is not binData but we find binData to be the result, this
        // means deserialization error happened. Push error to the DLQ if that is the requested
        // way to deal with the error.
        if (_options.keyFormat != KafkaKeyFormatEnum::BinData &&
            std::holds_alternative<std::vector<std::uint8_t>>(deserializedKey) &&
            _options.keyFormatError == KafkaSourceKeyFormatErrorEnum::Dlq) {
            sourceDoc.key = std::move(std::get<std::vector<std::uint8_t>>(deserializedKey));
            uasserted(ErrorCodes::BadValue,
                      str::stream() << "Failed to deserialize the Kafka key according to the "
                                       "specified key format '"
                                    << KafkaKeyFormat_serializer(_options.keyFormat) << "'");
        }
        streamMetaSource.setKey(std::move(deserializedKey));
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
                    "topicName"_attr = sourceDoc.topic,
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
    streamMetaSource.setTopic(sourceDoc.topic);
    streamMetaSource.setPartition(sourceDoc.partition);
    streamMetaSource.setOffset(sourceDoc.offset);
    streamMetaSource.setKey(deserializeKafkaKey(std::move(sourceDoc.key), _options.keyFormat));
    streamMetaSource.setHeaders(std::move(sourceDoc.headers));
    StreamMeta streamMeta;
    streamMeta.setSource(std::move(streamMetaSource));
    BSONObjBuilder objBuilder = streams::toDeadLetterQueueMsg(
        _context->streamMetaFieldName, streamMeta, getName(), std::move(sourceDoc.error));
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
        uassert(ErrorCodes::StreamProcessorInvalidOptions,
                "can only insert with FakeKafkaPartitionConsumer",
                bool(fakeKafkaPartition));

        std::vector<KafkaSourceDocument> docs;
        for (size_t j = partition; j < inputDocs.size(); j += numPartitions) {
            int64_t sizeBytes = inputDocs[j].objsize();
            docs.push_back(
                KafkaSourceDocument{.doc = std::move(inputDocs[j]),
                                    .messageSizeBytes = sizeBytes,
                                    .logAppendTimeMs = Date_t::now().toMillisSinceEpoch()});
        }
        fakeKafkaPartition->addDocuments(std::move(docs));
    }
}

std::unique_ptr<RdKafka::KafkaConsumer> KafkaConsumerOperator::createKafkaConsumer() {
    // Setup the resolve callback if so configured.
    if (_options.gwproxyEndpoint) {
        _resolveCbImpl = std::make_shared<KafkaResolveCallback>(
            _context, getName() /* operator name */, *_options.gwproxyEndpoint) /* target proxy */;

        // Setup the connect callback if authentication is required.
        if (_options.gwproxyKey) {
            _connectCbImpl = std::make_shared<KafkaConnectAuthCallback>(
                _context,
                getName() /* operator name */,
                *_options.gwproxyKey /* symmetricKey */,
                10 /* connection timeout unit:seconds */);
        }
    }

    return streams::createKafkaConsumer(_options.bootstrapServers,
                                        _options.consumerGroupId,
                                        _options.enableAutoCommit,
                                        _options.authConfig,
                                        _resolveCbImpl ? _resolveCbImpl.get() : nullptr,
                                        _connectCbImpl ? _connectCbImpl.get() : nullptr,
                                        nullptr);
}

// topic partition ids should have been fetched before calling getCommittedOffsets
KafkaConsumerOperator::TopicPartitionOffsetMap KafkaConsumerOperator::getCommittedOffsets() const {
    std::vector<std::unique_ptr<RdKafka::TopicPartition>> partitionsHolder;
    std::vector<RdKafka::TopicPartition*> partitions;

    invariant(!_topicPartitions.empty());
    partitionsHolder.reserve(_topicPartitions.size());
    partitions.reserve(_topicPartitions.size());

    for (const auto& [topic, partitionId] : _topicPartitions) {
        std::unique_ptr<RdKafka::TopicPartition> p;
        p.reset(RdKafka::TopicPartition::create(topic, partitionId));
        partitions.push_back(p.get());
        partitionsHolder.push_back(std::move(p));
    }

    if (_options.isTest) {
        return {};
    }

    tassert(8674601, "Expected _groupConsumer to be set", _groupConsumer);
    RdKafka::ErrorCode errCode = _groupConsumer->committed(partitions, kKafkaRequestTimeoutMs);
    uassert(ErrorCodes::StreamProcessorKafkaConnectionError,
            kafkaErrToString("KafkaConsumerOperator failed to get committed offsets", errCode),
            errCode == RdKafka::ERR_NO_ERROR);
    tassert(8385401,
            "KafkaConsumerOperator unexpected number of partitions received from the topic",
            partitions.size() == _topicPartitions.size());

    boost::optional<bool> hasValidOffsets;
    TopicPartitionOffsetMap committedOffsets;
    for (const auto& partition : partitions) {
        uassert(
            8385421,
            str::stream() << "KafkaConsumerOperator failed to get committed offset for partition: "
                          << partition->partition() << "; because partition has error: "
                          << RdKafka::err2str(partition->err()),
            partition->err() == RdKafka::ERR_NO_ERROR);
        if (partition->offset() != RdKafka::Topic::OFFSET_INVALID) {
            committedOffsets[{partition->topic(), partition->partition()}] = partition->offset();
            if (!hasValidOffsets) {
                hasValidOffsets = true;
            } else {
                // Ensure that either all partitions have a committed offset or all of them don't
                // have a committed offset, there shouldn't be a mix bag.
                tassert(8385402,
                        "KafkaConsumerOperator subset of partitions have a committed offset and a "
                        "subset do not have a committed offset",
                        *hasValidOffsets);
            }
        } else {
            if (!hasValidOffsets) {
                hasValidOffsets = false;
            } else {
                // Ensure that either all partitions have a committed offset or all of them don't
                // have a committed offset, there shouldn't be a mix bag.
                tassert(9358001,
                        "KafkaConsumerOperator subset of partitions have a committed offset and a "
                        "subset do not have a committed offset",
                        !*hasValidOffsets);
            }
        }
    }
    if (hasValidOffsets && *hasValidOffsets) {
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
        invariant(partitionState.getTopic());
        tp.reset(RdKafka::TopicPartition::create(std::string{*partitionState.getTopic()},
                                                 partitionState.getPartition()));
        tp->set_offset(partitionState.getOffset());
        topicPartitions.push_back(tp.get());
        topicPartitionsHolder.push_back(std::move(tp));
    }

    if (_groupConsumer && !_options.enableAutoCommit) {
        // Increment offsets here if we are not already doing so when reading from source.
        RdKafka::ErrorCode errCode = _groupConsumer->commitAsync(topicPartitions);
        uassert(8674605,
                kafkaErrToString("KafkaConsumerOperator failed to commit offsets", errCode),
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
    _stats.setMemoryUsageBytes(0);
    OperatorStats stats{_stats};
    for (auto& consumerInfo : _consumers) {
        stats += consumerInfo.consumer->getStats();
    }
    _stats.setMemoryUsageBytes(stats.memoryUsageBytes);
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
    std::string topicName,
    int32_t partition,
    int64_t startOffset,
    boost::optional<int64_t> rdkafkaQueuedMaxMessagesKBytes) {
    KafkaPartitionConsumerBase::Options options;
    options.bootstrapServers = _options.bootstrapServers;
    options.topicName = topicName;
    options.partition = partition;
    options.deserializer = _options.deserializer;
    options.maxNumDocsToReturn = _options.maxNumDocsToReturn;
    options.startOffset = startOffset;
    options.authConfig = _options.authConfig;
    options.kafkaRequestFailureSleepDurationMs = _options.kafkaRequestFailureSleepDurationMs;
    options.queueSizeGauge = _queueSizeGauge;
    options.queueByteSizeGauge = _queueByteSizeGauge;
    options.gwproxyEndpoint = _options.gwproxyEndpoint;
    options.gwproxyKey = _options.gwproxyKey;
    options.rdkafkaQueuedMaxMessagesKBytes = rdkafkaQueuedMaxMessagesKBytes;

    if (_options.isTest) {
        return std::make_unique<FakeKafkaPartitionConsumer>(_context, std::move(options));
    } else {
        return std::make_unique<KafkaPartitionConsumer>(_context, std::move(options));
    }
}

KafkaConsumerOperator::ConsumerInfo KafkaConsumerOperator::createPartitionConsumer(
    std::string topicName,
    int32_t partition,
    int64_t startOffset,
    boost::optional<int64_t> rdkafkaQueuedMaxMessagesKBytes) {
    ConsumerInfo consumerInfo;
    consumerInfo.partition = partition;
    consumerInfo.topic = topicName;
    consumerInfo.consumer = createKafkaPartitionConsumer(
        topicName, partition, startOffset, rdkafkaQueuedMaxMessagesKBytes);
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
        KafkaPartitionCheckpointState partitionState{
            consumerInfo.partition,
            checkpointStartingOffset,
        };
        partitionState.setTopic(consumerInfo.topic);
        if (_options.useWatermarks) {
            partitionState.setWatermark(WatermarkState{
                consumerInfo.watermarkGenerator->getWatermarkMsg().eventTimeWatermarkMs});
        }
        partitions.push_back(std::move(partitionState));
    }

    KafkaSourceCheckpointState state;
    state.setPartitions(std::move(partitions));
    state.setConsumerGroupId(_options.consumerGroupId);
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

        boost::optional<int64_t> offsetLag;
        auto brokerHighOffset = consumerInfo.consumer->getLatestOffsetAtBroker();
        if (brokerHighOffset && *brokerHighOffset >= currentOffset) {
            offsetLag = *brokerHighOffset - currentOffset;
        }

        bool isIdle{false};
        int64_t watermark{-1};
        if (consumerInfo.watermarkGenerator) {
            const auto& watermarkMsg = consumerInfo.watermarkGenerator->getWatermarkMsg();
            watermark = watermarkMsg.eventTimeWatermarkMs;
            isIdle = watermarkMsg.watermarkStatus == WatermarkStatus::kIdle;
        }

        states.push_back(
            KafkaConsumerPartitionState{.topic = consumerInfo.topic,
                                        .partition = consumerInfo.partition,
                                        .currentOffset = currentOffset,
                                        .checkpointOffset = consumerInfo.checkpointOffset,
                                        .partitionOffsetLag = offsetLag,
                                        .watermark = watermark,
                                        .isIdle = isIdle});
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
