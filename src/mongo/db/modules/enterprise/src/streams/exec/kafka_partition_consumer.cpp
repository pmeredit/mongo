/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/kafka_utils.h"
#include <chrono>
#include <rdkafka.h>
#include <rdkafkacpp.h>
#include <string>
#include <thread>

#include "mongo/base/error_codes.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/duration.h"
#include "mongo/util/str.h"
#include "streams/exec/context.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_event_callback.h"
#include "streams/exec/kafka_partition_consumer.h"
#include "streams/exec/log_util.h"
#include "streams/exec/operator.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/util.h"
#include "streams/util/exception.h"
#include "streams/util/units.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

// IMPORTANT! If you update this allowed list make sure you also update the UI that shows
// warnings for unsupported configurations. Keep in mind that there is also a list for
// allowed sink configurations that you may need to synchronize with as well.
// The UI logic was added in this PR https://github.com/10gen/mms/pull/117213
mongo::stdx::unordered_set<std::string> allowedSourcePartitionConfigurations = {
    "max.poll.records",
    "max.poll.interval.ms",
    "fetch.min.bytes",
    "fetch.max.bytes",
    "allow.auto.create.topics",
    "isolation.level",
    "client.dns.lookup",
    "max.partition.fetch.bytes",
    "connections.max.idle.ms",
    "exclude.internal.topics",
    "request.timeout.ms",
    // configurations shared with allowedSourceConfigurations
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "client.id",
};
namespace {

// This is the timeout we use for `consume_callback`, within librdkafka this is just
// used as the condvar timeout and not the actual consume/fetch request timeout. The
// fetching happens in a separate background thread within librdkafka.
static constexpr stdx::chrono::milliseconds kKafkaConsumeCallbackTimeoutMs{1'000};

};  // namespace

void KafkaPartitionConsumer::DocBatch::DocVec::pushDoc(KafkaSourceDocument doc) {
    dassert(size() < capacity());
    if (doc.doc) {
        if (!doc.doc->isEmpty()) {
            byteSize += doc.doc->sharedBuffer().capacity();
        }
    } else {
        byteSize += doc.messageSizeBytes;
    }
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
    auto& docVec = docVecs.back();
    byteSize += docVec.getByteSize();
}

void KafkaPartitionConsumer::DocBatch::pushDocToLastDocVec(KafkaSourceDocument doc) {
    auto& docVec = docVecs.back();
    dassert(docVec.size() < docVec.capacity());
    ++numDocs;
    int64_t byteSizePrev = docVec.getByteSize();
    docVec.pushDoc(std::move(doc));
    int64_t byteSizeDelta = docVec.getByteSize() - byteSizePrev;
    dassert(byteSizeDelta >= 0);
    byteSize += byteSizeDelta;
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
        bool hasError{true};
        try {
            _consumer->onMessage(msg);
            hasError = false;
        } catch (const SPException& e) {
            _consumer->onError(e.toStatus());
        } catch (const DBException& e) {
            _consumer->onError(e.toStatus());
        } catch (const std::exception& e) {
            LOGV2_ERROR(74677,
                        "{partition}: encountered exception: {error}",
                        "partition"_attr = _consumer->partition(),
                        "error"_attr = e.what());
            SPStatus status{
                Status{ErrorCodes::InternalError,
                       fmt::format("Kafka $source partition {} encountered unexpected error.",
                                   _consumer->partition())},
                e.what()};
            _consumer->onError(std::move(status));
        }

        if (hasError) {
            // Cancel the current callback dispatcher and immediately return the control
            // back to the consumer thread.
            rd_kafka_yield(nullptr);
        }
    }

    KafkaPartitionConsumer* _consumer{nullptr};
};

KafkaPartitionConsumer::KafkaPartitionConsumer(Context* context, Options options)
    : KafkaPartitionConsumerBase(context, std::move(options)),
      _memoryUsageHandle(_context->memoryAggregator->createUsageHandle()) {
    _eventCallback = std::make_unique<KafkaEventCallback>(
        _context, fmt::format("KafkaPartitionConsumer-{}", _options.partition));
}

void KafkaPartitionConsumer::doInit() {
    _conf = createKafkaConf();

    std::string errstr;
    _consumer.reset(RdKafka::Consumer::create(_conf.get(), errstr));
    uassert(ErrorCodes::StreamProcessorKafkaConnectionError,
            str::stream() << "Failed to create consumer with error: " << errstr,
            _consumer);

    _topic.reset(
        RdKafka::Topic::create(_consumer.get(), _options.topicName, /*conf*/ nullptr, errstr));
    uassert(ErrorCodes::StreamProcessorKafkaConnectionError,
            str::stream() << "Failed to create topic handle with error: " << errstr,
            _topic);
}

void KafkaPartitionConsumer::doStart() {
    dassert(!_consumerThread.joinable());
    _consumerThread = stdx::thread([this] {
        // Connect to Kafka.
        try {
            connectToSource();
            tassert(ErrorCodes::InternalError,
                    "Expected isConnected() to be true here.",
                    getConnectionStatus().isConnected());
        } catch (const SPException& e) {
            onConnectionError(e.toStatus());
        } catch (const DBException& e) {
            onConnectionError(e.toStatus());
        } catch (const std::exception& e) {
            onConnectionError(
                SPStatus{mongo::Status{ErrorCodes::InternalError,
                                       "Unexpected exception connecting to kafka $source."},
                         e.what()});
        }

        if (!_options.enableDataFlow) {
            // If data flow is disabled, don't actually read messages from the source.
            return;
        }

        // Start fetching messages.
        try {
            fetchLoop();
        } catch (const SPException& e) {
            onError(e.toStatus());
        } catch (const DBException& e) {
            onError(e.toStatus());
        } catch (const std::exception& e) {
            onError(SPStatus{mongo::Status{ErrorCodes::InternalError,
                                           "Unexpected exception reading from kafka $source."},
                             e.what()});
        }
    });
}

KafkaPartitionConsumer::~KafkaPartitionConsumer() {
    if (_consumerThread.joinable()) {
        {
            // Make sure that stop() has already been called if necessary.
            stdx::lock_guard<stdx::mutex> fLock(_finalizedDocBatch.mutex);
            dassert(_finalizedDocBatch.shutdown);
        }

        // Wait for the consumer thread to exit.
        _consumerThread.join();
    }
}

void KafkaPartitionConsumer::doStop() {
    uassert(ErrorCodes::InternalError, "_consumer is null", _consumer);
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
        stdx::lock_guard<stdx::mutex> fLock(_finalizedDocBatch.mutex);
        _finalizedDocBatch.shutdown = true;
        _consumerThreadWakeUpCond.notify_one();
    }
}

ConnectionStatus KafkaPartitionConsumer::doGetConnectionStatus() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _connectionStatus;
}

boost::optional<int64_t> KafkaPartitionConsumer::doGetStartOffset() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _startOffset;
}

std::vector<KafkaSourceDocument> KafkaPartitionConsumer::doGetDocuments() {
    std::vector<KafkaSourceDocument> docs;
    {
        stdx::lock_guard<stdx::mutex> fLock(_finalizedDocBatch.mutex);
        if (_finalizedDocBatch.empty()) {
            // Move docs from _activeDocBatch to _finalizedDocBatch.
            stdx::lock_guard<stdx::mutex> aLock(_activeDocBatch.mutex);
            while (!_activeDocBatch.empty()) {
                dassert(!_activeDocBatch.docVecs.empty());
                auto docVec = _activeDocBatch.popDocVec();
                _stats += {.memoryUsageBytes = docVec.getByteSize()};
                _memoryUsageHandle.set(_stats.memoryUsageBytes);
                _finalizedDocBatch.pushDocVec(std::move(docVec));
            }
        }

        spassert(_finalizedDocBatch.status, _finalizedDocBatch.status.isOK());

        if (!_finalizedDocBatch.empty()) {
            dassert(!_finalizedDocBatch.docVecs.empty());
            auto docVec = _finalizedDocBatch.popDocVec();
            _stats += {.memoryUsageBytes = -docVec.getByteSize()};
            _memoryUsageHandle.set(_stats.memoryUsageBytes);
            _options.queueSizeGauge->incBy(-docVec.size());
            _options.queueByteSizeGauge->incBy(-docVec.getByteSize());
            docs = std::move(docVec.docs);
        }

        // Make sure to signal _consumerThreadWakeUpCond on all exit paths.
        _consumerThreadWakeUpCond.notify_all();
    }
    return docs;
}

OperatorStats KafkaPartitionConsumer::doGetStats() {
    stdx::lock_guard<stdx::mutex> fLock(_finalizedDocBatch.mutex);
    return _stats;
}

std::unique_ptr<RdKafka::Conf> KafkaPartitionConsumer::createKafkaConf() {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto setConf = [confPtr = conf.get()](const std::string& confName, auto confValue) {
        std::string errstr;
        if (confPtr->set(confName, confValue, errstr) != RdKafka::Conf::CONF_OK) {
            uasserted(ErrorCodes::StreamProcessorKafkaConnectionError,
                      str::stream() << "Failed while setting configuration " << confName
                                    << " with error: " << errstr);
        }
    };

    setConf("bootstrap.servers", _options.bootstrapServers);
    if (streams::isConfluentBroker(_options.bootstrapServers)) {
        setConf("client.id", std::string(streams::kKafkaClientID));
    }
    if (!enableMetadataRefreshInterval(_context->featureFlags)) {
        // Do not refresh topic or broker metadata.
        setConf("topic.metadata.refresh.interval.ms", "-1");
        // Do not log broker disconnection messages.
        setConf("log.connection.close", "false");
    }
    setConf("enable.auto.commit", "false");
    setConf("enable.auto.offset.store", "false");
    setConf("consume.callback.max.messages", "500");

    // Set the resolve callback.
    if (_options.gwproxyEndpoint) {
        _resolveCbImpl =
            std::make_unique<KafkaResolveCallback>(_context,
                                                   "KafkaPartitionConsumer" /* operator name */,
                                                   *_options.gwproxyEndpoint /* target proxy */);
        setConf("resolve_cb", _resolveCbImpl.get());

        // Set the connect callback if authentication is required.
        if (_options.gwproxyKey) {
            _connectCbImpl = std::make_unique<KafkaConnectAuthCallback>(
                _context,
                "KafkaPartitionConsumer" /* operator name */,
                *_options.gwproxyKey /* symmetric key */,
                10 /* connection timeout unit:seconds */);
            setConf("connect_cb", _connectCbImpl.get());
        }
    }

    setConf("event_cb", _eventCallback.get());

    // Set auth related configurations.
    for (const auto& config : _options.authConfig) {
        setConf(config.first, config.second);
    }

    // These are the configurations that the user manually specified in the kafka connection.
    if (_options.configurations) {
        setKafkaConnectionConfigurations(
            *_options.configurations, setConf, allowedSourcePartitionConfigurations);
    }

    if (_options.rdkafkaQueuedMaxMessagesKBytes) {
        setConf("queued.max.messages.kbytes",
                std::to_string(*_options.rdkafkaQueuedMaxMessagesKBytes));
        // Use a fetch.max.bytes that is smaller than the queued.max.messages.kbytes.
        int64_t fetchMaxBytes = (*_options.rdkafkaQueuedMaxMessagesKBytes * 1024) - 64_KiB;
        setConf("fetch.max.bytes", std::to_string(fetchMaxBytes));
        LOGV2_INFO(9649600,
                   "Setting rdkafka queue size",
                   "queuedMaxMessagesKBytes"_attr = *_options.rdkafkaQueuedMaxMessagesKBytes,
                   "fetchMaxBytes"_attr = fetchMaxBytes,
                   "context"_attr = _context,
                   "partition"_attr = _options.partition);
    }

    // Set debug contexts to get more information from librdkafka
    setConf("debug", "security");

    return conf;
}

int64_t KafkaPartitionConsumer::queryWatermarkOffsets() {
    int64_t startOffset = _options.startOffset;
    if (startOffset == RdKafka::Topic::OFFSET_BEGINNING ||
        startOffset == RdKafka::Topic::OFFSET_END) {
        // The user wants us to start from the the current beginning or end of the topic.
        // We retrieve the current beginning and end with query_watermark_offsets.
        int64_t lowOffset = 0;
        int64_t highOffset = 0;
        RdKafka::ErrorCode resp = _consumer->query_watermark_offsets(
            _topic->name(), _options.partition, &lowOffset, &highOffset, kKafkaRequestTimeoutMs);
        if (resp != RdKafka::ERR_NO_ERROR) {
            auto errStr = kafkaErrToString(
                fmt::format("query_watermark_offsets failed for partition {}", partition()), resp);
            uasserted(ErrorCodes::StreamProcessorKafkaConnectionError, errStr);
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

boost::optional<std::string> KafkaPartitionConsumer::getVerboseCallbackErrorsIfExists() {
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

void KafkaPartitionConsumer::connectToSource() {
    auto startOffset = queryWatermarkOffsets();

    RdKafka::ErrorCode resp = _consumer->start(_topic.get(), _options.partition, startOffset);

    const boost::optional<std::string> formattedErrors = getVerboseCallbackErrorsIfExists();
    uassert(ErrorCodes::StreamProcessorKafkaConnectionError,
            formattedErrors ? formattedErrors.get()
                            : kafkaErrToString("Failed to start consumer with error", resp),
            resp == RdKafka::ERR_NO_ERROR);

    LOGV2_INFO(9219600,
               "KafkaPartitionConsumer started",
               "context"_attr = _context,
               "rdkafkaQueuedMaxMessagesKBytes"_attr = _options.rdkafkaQueuedMaxMessagesKBytes,
               "partition"_attr = partition());

    // Set the state to connected.
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _connectionStatus = ConnectionStatus{ConnectionStatus::Status::kConnected};
    _startOffset = startOffset;
}

void KafkaPartitionConsumer::fetchLoop() {
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

            // Report current memory usage to SourceBufferManager and allocate one page of memory
            // from it.
            bool allocSuccess = _context->sourceBufferManager->allocPages(
                _sourceBufferHandle.get(), _stats.memoryUsageBytes /* curSize */, 1 /* numPages */);
            if (!allocSuccess) {
                LOGV2_DEBUG(74678,
                            1,
                            "{partition}: waiting for consumer",
                            "context"_attr = _context,
                            "partition"_attr = partition(),
                            "docsBuffered"_attr = _finalizedDocBatch.size(),
                            "bytesBuffered"_attr = _finalizedDocBatch.getByteSize(),
                            "numDocsReturned"_attr = _finalizedDocBatch.numDocsReturned);

                _consumerThreadWakeUpCond.wait(fLock);
                LOGV2_DEBUG(74679,
                            1,
                            "{partition}: waking up when bytesBuffered: {bytesBuffered}"
                            " numDocsReturned: {numDocsReturned}",
                            "context"_attr = _context,
                            "partition"_attr = partition(),
                            "numDocs"_attr = _finalizedDocBatch.numDocs,
                            "bytesBuffered"_attr = _finalizedDocBatch.getByteSize(),
                            "numDocsReturned"_attr = _finalizedDocBatch.numDocsReturned);
                continue;  // Retry SourceBufferManager::allocPages().
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
            onError(SPStatus{{ErrorCodes::StreamProcessorKafkaConnectionError,
                              fmt::format("Kafka $source partition {} consume_callback() failed",
                                          partition())}});
            // At this point onError has been called, and the next call to doGetDocuments will error
            // out the processor.
            return;
        } else {
            LOGV2_DEBUG(74680,
                        1,
                        "{partition}: numDocsFetched by consume_callback(): {numDocsFetched}",
                        "context"_attr = _context,
                        "partition"_attr = partition(),
                        "numDocsFetched"_attr = numDocsFetched);
        }

        // Here we ask the eventCallback if we should error out.
        // Even if the broker is down, the above consume_callback will still work fine.
        // But the eventCallback will receive an ALL_BROKERS_DOWN error, and this will return true.
        uassert(ErrorCodes::StreamProcessorKafkaConnectionError,
                fmt::format("Kafka $source partition {} encountered error", partition()),
                !_eventCallback->hasError());

        // TODO(sandeep): Test to see if we really need to call poll(). Also, move this to
        // another background thread that polls on behalf of all the consumers in the process.
        _consumer->poll(0);
    }
}

boost::optional<int64_t> KafkaPartitionConsumer::doGetLatestOffsetAtBroker() const {
    int64_t low = RdKafka::Topic::OFFSET_INVALID;
    int64_t high = RdKafka::Topic::OFFSET_INVALID;

    // From librdkafa documentation, this is a local call and returns cached
    // values for the low/high offsets for the given topic/partition.
    // The cached high offset is updated for every message that is received
    // by librdkafa from the broker. The cached low offset is updated less frequently
    // but we are only going to use the cached high offset here.
    auto err =
        _consumer->get_watermark_offsets(_options.topicName, _options.partition, &low, &high);

    if (err != RdKafka::ERR_NO_ERROR) {
        LOGV2_INFO(9092701,
                   "Could not get latest offset at broker",
                   "context"_attr = _context,
                   "partition"_attr = _options.partition,
                   "err"_attr = err);
        return boost::none;
    }

    return high;
}

void KafkaPartitionConsumer::pushDocToActiveDocBatch(KafkaSourceDocument doc) {
    int32_t numActiveDocVecs{0};
    {
        stdx::lock_guard<stdx::mutex> aLock(_activeDocBatch.mutex);
        int64_t prevSize = _activeDocBatch.size();
        int64_t prevByteSize = _activeDocBatch.getByteSize();

        if (_activeDocBatch.docVecs.empty()) {
            _activeDocBatch.emplaceDocVec(_options.maxNumDocsToReturn);
        }

        // Assert that there is capacity available in the last DocVec.
        auto& activeDocVec = _activeDocBatch.docVecs.back();
        dassert(activeDocVec.size() < _options.maxNumDocsToReturn);
        _activeDocBatch.pushDocToLastDocVec(std::move(doc));

        if (isDocVecFull(activeDocVec)) {
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
        stdx::lock_guard<stdx::mutex> fLock(_finalizedDocBatch.mutex);
        stdx::lock_guard<stdx::mutex> aLock(_activeDocBatch.mutex);
        while (!_activeDocBatch.docVecs.empty()) {
            // Avoid pushing DocVec into _finalizedDocBatch until it's full.
            if (isDocVecFull(_activeDocBatch.docVecs.front())) {
                auto docVec = _activeDocBatch.popDocVec();
                _stats += {.memoryUsageBytes = docVec.getByteSize()};
                _memoryUsageHandle.set(_stats.memoryUsageBytes);
                _finalizedDocBatch.pushDocVec(std::move(docVec));
            } else {
                break;
            }
        }
    }
}

bool KafkaPartitionConsumer::isDocVecFull(const DocBatch::DocVec& docVec) const {
    if (docVec.size() >= _options.maxNumDocsToReturn) {
        return true;
    }
    if (!docVec.empty() && docVec.getByteSize() >= kDataMsgMaxByteSize) {
        return true;
    }
    return false;
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
            uasserted(ErrorCodes::StreamProcessorKafkaConnectionError,
                      str::stream() << "Failed to consume with error: " << message.errstr());
            break;
        }
    }
}

void KafkaPartitionConsumer::onError(SPStatus status) {
    status = _eventCallback->appendRecentErrorsToStatus(status);
    stdx::lock_guard<stdx::mutex> fLock(_finalizedDocBatch.mutex);
    _finalizedDocBatch.shutdown = true;
    if (_finalizedDocBatch.status.isOK()) {
        _finalizedDocBatch.status = std::move(status);
    }
}

void KafkaPartitionConsumer::onConnectionError(SPStatus status) {
    status = _eventCallback->appendRecentErrorsToStatus(status);
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _connectionStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
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

    sourceDoc.topic = topicName();
    sourceDoc.partition = partition();
    sourceDoc.offset = message.offset();
    sourceDoc.messageSizeBytes = message.len();
    auto keyPointer = static_cast<const uint8_t*>(message.key_pointer());
    if (keyPointer) {
        auto keyLen = message.key_len();
        sourceDoc.key.emplace();
        sourceDoc.key->reserve(keyLen);
        sourceDoc.key->assign(keyPointer, keyPointer + keyLen);
    }
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
