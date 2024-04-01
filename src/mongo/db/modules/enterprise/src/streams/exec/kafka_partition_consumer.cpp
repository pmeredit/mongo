/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <chrono>
#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>
#include <thread>

#include "mongo/base/error_codes.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/str.h"
#include "streams/exec/context.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_event_callback.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/log_util.h"
#include "streams/exec/operator.h"
#include "streams/exec/stream_stats.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

// This is the timeout we use for `consume_callback`, within librdkafka this is just
// used as the condvar timeout and not the actual consume/fetch request timeout. The
// fetching happens in a separate background thread within librdkafka.
static constexpr stdx::chrono::milliseconds kKafkaConsumeCallbackTimeoutMs{1'000};

};  // namespace

void KafkaPartitionConsumer::DocBatch::DocVec::pushDoc(KafkaSourceDocument doc) {
    dassert(size() < capacity());
    byteSize += doc.sizeBytes;
    docs.push_back(std::move(doc));
}

int64_t KafkaPartitionConsumer::DocBatch::size() const {
    dassert(numDocs >= 0);
    return numDocs;
}

int64_t KafkaPartitionConsumer::DocBatch::getByteSize() const {
    dassert(byteSize >= 0);
    return byteSize;
}

bool KafkaPartitionConsumer::DocBatch::empty() const {
    dassert(numDocs >= 0);
    return numDocs == 0;
}

void KafkaPartitionConsumer::DocBatch::emplaceDocVec(size_t capacity) {
    docVecs.emplace(capacity);
}

void KafkaPartitionConsumer::DocBatch::pushDocToLastDocVec(KafkaSourceDocument doc) {
    auto& docVec = docVecs.back();
    dassert(docVec.size() < docVec.capacity());
    ++numDocs;
    byteSize -= docVec.getByteSize();
    dassert(byteSize >= 0);
    docVec.pushDoc(std::move(doc));
    byteSize += docVec.getByteSize();
}

void KafkaPartitionConsumer::DocBatch::pushDocVec(DocVec docVec) {
    numDocs += docVec.size();
    byteSize += docVec.getByteSize();
    docVecs.push(std::move(docVec));
}

auto KafkaPartitionConsumer::DocBatch::popDocVec() -> DocVec {
    auto docVec = std::move(docVecs.front());
    docVecs.pop();
    dassert(!docVec.docs.empty());
    numDocs -= docVec.size();
    byteSize -= docVec.getByteSize();
    numDocsReturned += docVec.size();
    return docVec;
}

// This class is used with RdKafka::Consumer::consume_callback() to receive input docs from Kafka.
// Simply calls KafkaPartitionConsumer::onMessage() for every incoming message.
class ConsumeCbImpl : public RdKafka::ConsumeCb {
public:
    ConsumeCbImpl(KafkaPartitionConsumer* consumer) : _consumer(consumer) {}

private:
    void consume_cb(RdKafka::Message& msg, void* opaque) override {
        dassert(opaque == nullptr);
        try {
            _consumer->onMessage(msg);
        } catch (const std::exception& e) {
            LOGV2_ERROR(74677,
                        "{partition}: encountered exception: {error}",
                        "partition"_attr = _consumer->partition(),
                        "error"_attr = e.what());
            _consumer->onError(std::current_exception());

            // Cancel the current callback dispatcher and immediately return the control
            // back to the consumer thread.
            rd_kafka_yield(nullptr);
        }
    }

    KafkaPartitionConsumer* _consumer{nullptr};
};

KafkaPartitionConsumer::KafkaPartitionConsumer(Context* context, Options options)
    : KafkaPartitionConsumerBase(std::move(options)),
      _context(context),
      _memoryUsageHandle(_context->memoryAggregator->createUsageHandle()) {
    _eventCbImpl = std::make_unique<KafkaEventCallback>(
        _context, fmt::format("KafkaPartitionConsumer-{}", _options.partition));
}

void KafkaPartitionConsumer::doInit() {
    _conf = createKafkaConf();

    std::string errstr;
    _consumer.reset(RdKafka::Consumer::create(_conf.get(), errstr));
    uassert(
        8720705, str::stream() << "Failed to create consumer with error: " << errstr, _consumer);

    _topic.reset(
        RdKafka::Topic::create(_consumer.get(), _options.topicName, /*conf*/ nullptr, errstr));
    uassert(
        8720706, str::stream() << "Failed to create topic handle with error: " << errstr, _topic);
}

void KafkaPartitionConsumer::doStart() {
    dassert(!_consumerThread.joinable());
    _consumerThread = stdx::thread([this] { fetchLoop(); });
}

KafkaPartitionConsumer::~KafkaPartitionConsumer() {
    if (_consumerThread.joinable()) {
        {
            // Make sure that stop() has already been called if necessary.
            stdx::lock_guard<Latch> fLock(_finalizedDocBatch.mutex);
            dassert(_finalizedDocBatch.shutdown);
        }

        // Wait for the consumer thread to exit.
        _consumerThread.join();
    }
}

void KafkaPartitionConsumer::doStop() {
    // Stop the consumer first before shutting down our consumer thread. Stopping
    // the consumer will purge the entire buffered queue within rdkafka and force
    // `consume_callback` to exit quickly.
    RdKafka::ErrorCode resp = _consumer->stop(_topic.get(), _options.partition);
    if (resp != RdKafka::ERR_NO_ERROR) {
        LOGV2_ERROR(76435,
                    "{partition}: stop() failed: {error}",
                    "context"_attr = _context,
                    "partition"_attr = partition(),
                    "error"_attr = RdKafka::err2str(resp));
    }

    if (_consumerThread.joinable()) {
        stdx::lock_guard<Latch> fLock(_finalizedDocBatch.mutex);
        _finalizedDocBatch.shutdown = true;
        _consumerThreadWakeUpCond.notify_one();
    }
}

ConnectionStatus KafkaPartitionConsumer::doGetConnectionStatus() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _connectionStatus;
}

void KafkaPartitionConsumer::setConnectionStatus(ConnectionStatus status) {
    stdx::lock_guard<Latch> lock(_mutex);
    _connectionStatus = status;
}

boost::optional<int64_t> KafkaPartitionConsumer::doGetStartOffset() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _startOffset;
}

boost::optional<int64_t> KafkaPartitionConsumer::doGetNumPartitions() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _numPartitions;
}

std::vector<KafkaSourceDocument> KafkaPartitionConsumer::doGetDocuments() {
    std::vector<KafkaSourceDocument> docs;
    {
        stdx::lock_guard<Latch> fLock(_finalizedDocBatch.mutex);
        if (_finalizedDocBatch.empty()) {
            // Move docs from _activeDocBatch to _finalizedDocBatch.
            stdx::lock_guard<Latch> aLock(_activeDocBatch.mutex);
            while (!_activeDocBatch.empty()) {
                dassert(!_activeDocBatch.docVecs.empty());
                auto docVec = _activeDocBatch.popDocVec();
                _memoryUsageHandle.add(docVec.getByteSize());
                _finalizedDocBatch.pushDocVec(std::move(docVec));
            }
        }

        if (_finalizedDocBatch.exception) {
            // Throw the exception to the caller.
            std::rethrow_exception(_finalizedDocBatch.exception);
        }
        if (!_finalizedDocBatch.empty()) {
            dassert(!_finalizedDocBatch.docVecs.empty());
            auto docVec = _finalizedDocBatch.popDocVec();
            _memoryUsageHandle.add(-docVec.getByteSize());
            _options.queueSizeGauge->incBy(-docVec.size());
            _options.queueByteSizeGauge->incBy(-docVec.getByteSize());
            docs = std::move(docVec.docs);
        }
        _consumerThreadWakeUpCond.notify_all();
    }
    return docs;
}

OperatorStats KafkaPartitionConsumer::doGetStats() {
    return OperatorStats{.memoryUsageBytes = _memoryUsageHandle.getCurrentMemoryUsageBytes()};
}


std::unique_ptr<RdKafka::Conf> KafkaPartitionConsumer::createKafkaConf() {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto setConf = [confPtr = conf.get()](const std::string& confName, auto confValue) {
        std::string errstr;
        if (confPtr->set(confName, confValue, errstr) != RdKafka::Conf::CONF_OK) {
            uasserted(8720708,
                      str::stream() << "Failed while setting configuration " << confName
                                    << " with error: " << errstr);
        }
    };
    setConf("bootstrap.servers", _options.bootstrapServers);
    // Do not log broker disconnection messages.
    setConf("log.connection.close", "false");
    // Do not refresh topic or broker metadata.
    setConf("topic.metadata.refresh.interval.ms", "-1");
    setConf("enable.auto.commit", "false");
    setConf("enable.auto.offset.store", "false");

    setConf("event_cb", _eventCbImpl.get());

    // Set auth related configurations.
    for (const auto& config : _options.authConfig) {
        setConf(config.first, config.second);
    }

    // TODO(sandeep): Set more config options that could be useful.
    return conf;
}

boost::optional<int64_t> KafkaPartitionConsumer::queryWatermarkOffsets() {
    int64_t startOffset = _options.startOffset;
    if (startOffset == RdKafka::Topic::OFFSET_BEGINNING ||
        startOffset == RdKafka::Topic::OFFSET_END) {
        // The user wants us to start from the the current beginning or end of the topic.
        // We retrieve the current beginning and end with query_watermark_offsets.
        int64_t lowOffset = 0;
        int64_t highOffset = 0;
        RdKafka::ErrorCode resp =
            _consumer->query_watermark_offsets(_topic->name(),
                                               _options.partition,
                                               &lowOffset,
                                               &highOffset,
                                               _options.kafkaRequestTimeoutMs.count());
        if (resp != RdKafka::ERR_NO_ERROR) {
            LOGV2_ERROR(76434,
                        "{partition}: query_watermark_offsets() failed: {error}",
                        "context"_attr = _context,
                        "partition"_attr = partition(),
                        "error"_attr = RdKafka::err2str(resp));
            return boost::none;
        }

        if (startOffset == RdKafka::Topic::OFFSET_BEGINNING) {
            startOffset = lowOffset;
        } else {
            // Kafka will return the current "end of topic" offset. So if there's 2 messages in
            // the topic at offset 0 and offset 1, the highOffset will be 2.
            startOffset = highOffset;
        }
        LOGV2_INFO(74683,
                   "query_watermark_offsets succeeded",
                   "context"_attr = _context,
                   "topicName"_attr = _options.topicName,
                   "partition"_attr = _options.partition,
                   "lowOffset"_attr = lowOffset,
                   "highOffset"_attr = highOffset,
                   "startOffset"_attr = startOffset);
    }
    return startOffset;
}

void KafkaPartitionConsumer::connectToSource() {
    boost::optional<int64_t> startOffset;
    boost::optional<int64_t> numPartitions;
    try {
        while (true) {
            {
                stdx::unique_lock fLock(_finalizedDocBatch.mutex);
                if (_finalizedDocBatch.shutdown) {
                    LOGV2_INFO(74681,
                               "{partition}: exiting fetchLoop()",
                               "partition"_attr = partition(),
                               "context"_attr = _context);
                    return;
                }
            }

            bool success{false};
            RdKafka::Metadata* metadata{nullptr};
            RdKafka::ErrorCode resp = _consumer->metadata(/*all_topics*/ false,
                                                          _topic.get(),
                                                          &metadata,
                                                          _options.kafkaRequestTimeoutMs.count());

            std::unique_ptr<RdKafka::Metadata> metadataOwner(metadata);
            if (resp != RdKafka::ERR_NO_ERROR || metadata->topics()->size() != 1) {
                LOGV2_ERROR(77179,
                            "{partition}: could not load metadata for the topic: {error}",
                            "context"_attr = _context,
                            "topic"_attr = _options.topicName,
                            "partition"_attr = partition(),
                            "error"_attr = RdKafka::err2str(resp));
                setConnectionStatus(
                    ConnectionStatus{ConnectionStatus::Status::kError,
                                     {{ErrorCodes::Error(77175),
                                       fmt::format("Could not connect to the Kafka topic with "
                                                   "kafka error code: {}, message: {}.",
                                                   resp,
                                                   RdKafka::err2str(resp))}}});
            } else if (metadata->topics()->at(0)->partitions()->empty()) {
                // TODO(SERVER-80865): Clarify the behavior when the topic does not (yet) exist.
                // Can we error out in this case or do we need to indefinitely wait for it to exist?
                LOGV2_ERROR(77178,
                            "topic does not exist",
                            "context"_attr = _context,
                            "topic"_attr = _options.topicName,
                            "partition"_attr = partition());
                setConnectionStatus(
                    ConnectionStatus{ConnectionStatus::Status::kError,
                                     {{ErrorCodes::Error(77176),
                                       "No partitions found in topic. Does the topic exist?"}}});
            } else {
                numPartitions = metadata->topics()->at(0)->partitions()->size();
                success = true;
            }

            if (success) {
                startOffset = queryWatermarkOffsets();
                success = bool(startOffset);
            }

            if (success) {
                break;
            } else {
                // Necessary metadata could not be fetched in this run, so sleep a little before
                // retrying.
                stdx::this_thread::sleep_for(
                    stdx::chrono::milliseconds(_options.kafkaRequestFailureSleepDurationMs));
            }
        }

        uassert(74688, "Expected startingOffset greater than or equal to zero", *startOffset >= 0);

        RdKafka::ErrorCode resp = _consumer->start(_topic.get(), _options.partition, *startOffset);
        uassert(8720709,
                str::stream() << "Failed to start consumer with error: " << RdKafka::err2str(resp),
                resp == RdKafka::ERR_NO_ERROR);

        setConnectionStatus(ConnectionStatus{ConnectionStatus::Status::kConnected});
        stdx::lock_guard<Latch> lock(_mutex);
        _startOffset = startOffset;
        _numPartitions = numPartitions;
    } catch (const std::exception& e) {
        LOGV2_ERROR(76444,
                    "{partition}: encountered exception: {error}",
                    "context"_attr = _context,
                    "partition"_attr = partition(),
                    "error"_attr = e.what());
        onError(std::current_exception());
    }
}

void KafkaPartitionConsumer::fetchLoop() {
    connectToSource();
    if (!getConnectionStatus().isConnected()) {
        LOGV2_INFO(76445,
                   "{partition}: exiting fetchLoop()",
                   "context"_attr = _context,
                   "partition"_attr = partition());
        return;
    }

    ConsumeCbImpl consumeCbImpl(this);
    while (true) {
        {
            stdx::unique_lock fLock(_finalizedDocBatch.mutex);
            if (_finalizedDocBatch.shutdown) {
                LOGV2_INFO(76436,
                           "{partition}: exiting fetchLoop()",
                           "context"_attr = _context,
                           "partition"_attr = partition());
                return;
            }

            if (_finalizedDocBatch.getByteSize() >= _options.maxPrefetchByteSize) {
                LOGV2_DEBUG(74678,
                            1,
                            "{partition}: waiting when bytesBuffered: {bytesBuffered}"
                            " numDocsReturned: {numDocsReturned}",
                            "context"_attr = _context,
                            "partition"_attr = partition(),
                            "numDocs"_attr = _finalizedDocBatch.numDocs,
                            "bytesBuffered"_attr = _finalizedDocBatch.getByteSize(),
                            "numDocsReturned"_attr = _finalizedDocBatch.numDocsReturned);
                _consumerThreadWakeUpCond.wait(fLock, [this]() {
                    return _finalizedDocBatch.shutdown ||
                        _finalizedDocBatch.getByteSize() < _options.maxPrefetchByteSize;
                });
                LOGV2_DEBUG(74679,
                            1,
                            "{partition}: waking up when bytesBuffered: {bytesBuffered}"
                            " numDocsReturned: {numDocsReturned}",
                            "context"_attr = _context,
                            "partition"_attr = partition(),
                            "numDocs"_attr = _finalizedDocBatch.numDocs,
                            "bytesBuffered"_attr = _finalizedDocBatch.getByteSize(),
                            "numDocsReturned"_attr = _finalizedDocBatch.numDocsReturned);
            }
        }

        // TODO(sandeep): During local testing, I noticed that numDocsFetched
        // can be higher than actual number of docs fetched. Does it matter? Can we fix it?
        // TODO(sandeep): Test that it is ok to not call consume_callback() for an extended
        // period of time.
        int numDocsFetched = _consumer->consume_callback(_topic.get(),
                                                         _options.partition,
                                                         kKafkaConsumeCallbackTimeoutMs.count(),
                                                         &consumeCbImpl,
                                                         /*opaque*/ nullptr);
        if (numDocsFetched < 0) {
            LOGV2_ERROR(76438,
                        "{partition}: consume_callback() failed",
                        "partition"_attr = partition(),
                        "context"_attr = _context);
            setConnectionStatus(ConnectionStatus{
                ConnectionStatus::Status::kError,
                {{ErrorCodes::Error(76437),
                  fmt::format("partition: {} consume_callback() failed", partition())}}});
            // Sleep a little before retrying.
            stdx::this_thread::sleep_for(
                stdx::chrono::milliseconds(_options.kafkaRequestFailureSleepDurationMs));
        } else {
            LOGV2_DEBUG(74680,
                        1,
                        "{partition}: numDocsFetched by consume_callback(): {numDocsFetched}",
                        "context"_attr = _context,
                        "partition"_attr = partition(),
                        "numDocsFetched"_attr = numDocsFetched);
        }
        // TODO(sandeep): Test to see if we really need to call poll(). Also, move this to
        // another background thread that polls on behalf of all the consumers in the process.
        _consumer->poll(0);
    }
}

void KafkaPartitionConsumer::pushDocToActiveDocBatch(KafkaSourceDocument doc) {
    int32_t numActiveDocVecs{0};
    {
        stdx::lock_guard<Latch> aLock(_activeDocBatch.mutex);
        if (_activeDocBatch.docVecs.empty()) {
            _activeDocBatch.emplaceDocVec(_options.maxNumDocsToReturn);
        }

        int64_t prevSize = _activeDocBatch.size();
        int64_t prevByteSize = _activeDocBatch.getByteSize();

        // Assert that there is capacity available in the last DocVec.
        auto& activeDocVec = _activeDocBatch.docVecs.back();
        dassert(activeDocVec.size() < _options.maxNumDocsToReturn);
        _activeDocBatch.pushDocToLastDocVec(std::move(doc));

        if (activeDocVec.size() == _options.maxNumDocsToReturn ||
            activeDocVec.getByteSize() >= kDataMsgMaxByteSize) {
            _activeDocBatch.emplaceDocVec(_options.maxNumDocsToReturn);
        }
        numActiveDocVecs = _activeDocBatch.docVecs.size();

        int64_t newSize = _activeDocBatch.size();
        int64_t newByteSize = _activeDocBatch.getByteSize();
        _options.queueSizeGauge->incBy(newSize - prevSize);
        _options.queueByteSizeGauge->incBy(newByteSize - prevByteSize);
    }

    if (numActiveDocVecs > 1) {
        // Move all full DocVecs from _activeDocBatch to _finalizedDocBatch.
        // Note that we released _activeDocBatch.mutex above, so the state of _activeDocBatch
        // could now be different.
        stdx::lock_guard<Latch> fLock(_finalizedDocBatch.mutex);
        stdx::lock_guard<Latch> aLock(_activeDocBatch.mutex);
        while (!_activeDocBatch.docVecs.empty()) {
            if (_activeDocBatch.docVecs.front().size() < _options.maxNumDocsToReturn &&
                _activeDocBatch.docVecs.front().getByteSize() < kDataMsgMaxByteSize) {
                // Avoid pushing DocVec into _finalizedDocBatch until it's full.
                // At this point, the first DocVec still has capacity.
                dassert(_activeDocBatch.docVecs.size() == 1);
                break;
            }

            auto docVec = _activeDocBatch.popDocVec();
            _memoryUsageHandle.add(docVec.getByteSize());
            _finalizedDocBatch.pushDocVec(std::move(docVec));
        }
    }
}

void KafkaPartitionConsumer::onMessage(RdKafka::Message& message) {
    switch (message.err()) {
        case RdKafka::ERR__TIMED_OUT:
            break;  // Do nothing.
        case RdKafka::ERR_NO_ERROR: {
            pushDocToActiveDocBatch(processMessagePayload(message));
            break;
        }
        default: {
            uasserted(8720711,
                      str::stream() << "Failed to consume with error: " << message.errstr());
            break;
        }
    }
}

void KafkaPartitionConsumer::onError(std::exception_ptr exception) {
    {
        stdx::lock_guard<Latch> fLock(_finalizedDocBatch.mutex);
        _finalizedDocBatch.shutdown = true;
        if (!_finalizedDocBatch.exception) {
            _finalizedDocBatch.exception = std::move(exception);
        }
    }
}

KafkaSourceDocument KafkaPartitionConsumer::processMessagePayload(RdKafka::Message& message) {
    dassert(message.err() == RdKafka::ERR_NO_ERROR);

    KafkaSourceDocument sourceDoc;
    const char* buf = static_cast<const char*>(message.payload());
    try {
        sourceDoc.doc = _options.deserializer->deserialize(buf, message.len());
    } catch (const std::exception& e) {
        LOGV2_ERROR(74682,
                    "{partition}: Failed to parse input message: {error}",
                    "context"_attr = _context,
                    "partition"_attr = partition(),
                    "error"_attr = e.what());
        sourceDoc.doc = boost::none;
        sourceDoc.error = str::stream() << "Failed to parse input message with error:" << e.what();
    }

    sourceDoc.partition = partition();
    sourceDoc.offset = message.offset();
    sourceDoc.sizeBytes = message.len();
    if (_options.enableKeysAndHeaders) {
        auto keyPointer = static_cast<const uint8_t*>(message.key_pointer());
        auto keyLen = message.key_len();
        sourceDoc.key.reserve(keyLen);
        sourceDoc.key.assign(keyPointer, keyPointer + keyLen);
        auto msgHeaders = message.headers();
        if (msgHeaders) {
            for (auto&& msgHeader : msgHeaders->get_all()) {
                std::vector<uint8_t> value;
                auto valuePointer = static_cast<const uint8_t*>(msgHeader.value());
                auto valueLen = msgHeader.value_size();
                value.reserve(valueLen);
                value.assign(valuePointer, valuePointer + valueLen);
                mongo::KafkaHeader header{
                    msgHeader.key(),
                    std::move(value),
                };
                sourceDoc.headers.emplace_back(std::move(header));
            }
        }
    }
    // TODO: https://jira.mongodb.org/browse/STREAMS-245
    // We should clarify the behavior here later. For now,
    // we let either MSG_TIMESTAMP_CREATE_TIME or MSG_TIMESTAMP_LOG_APPEND_TIME
    // take the sourceDoc.logAppendTimeMs, and thus the official _ts of the document.
    if (message.timestamp().type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME ||
        message.timestamp().type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME) {
        sourceDoc.logAppendTimeMs = message.timestamp().timestamp;
    }
    return sourceDoc;
}

}  // namespace streams
