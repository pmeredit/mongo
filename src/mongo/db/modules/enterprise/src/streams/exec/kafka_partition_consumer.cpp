/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <chrono>
#include <rdkafka.h>
#include <string>

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/str.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_partition_consumer.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

int32_t KafkaPartitionConsumer::DocBatch::size() const {
    dassert(numDocs >= 0);
    return numDocs;
}

bool KafkaPartitionConsumer::DocBatch::empty() const {
    dassert(numDocs >= 0);
    return numDocs == 0;
}

void KafkaPartitionConsumer::DocBatch::emplaceDocVec(size_t capacity) {
    docVecs.emplace();
    docVecs.back().reserve(capacity);
}

void KafkaPartitionConsumer::DocBatch::pushDocToLastDocVec(KafkaSourceDocument doc) {
    dassert(docVecs.back().size() < docVecs.back().capacity());
    docVecs.back().push_back(std::move(doc));
    ++numDocs;
}

void KafkaPartitionConsumer::DocBatch::pushDocVec(std::vector<KafkaSourceDocument> docVec) {
    numDocs += docVec.size();
    docVecs.push(std::move(docVec));
}

std::vector<KafkaSourceDocument> KafkaPartitionConsumer::DocBatch::popDocVec() {
    std::vector<KafkaSourceDocument> docVec = std::move(docVecs.front());
    docVecs.pop();
    dassert(!docVec.empty());
    numDocs -= docVec.size();
    numDocsReturned += docVec.size();
    return docVec;
}

// Simply calls KafkaPartitionConsumer::consumeCb for every incoming message.
// TODO(sandeep): Also implement custom RdKafka::EventCb.
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

KafkaPartitionConsumer::KafkaPartitionConsumer(Options options) : _options(std::move(options)) {}

void KafkaPartitionConsumer::doInit() {
    _conf = createKafkaConf();

    std::string errstr;
    _consumer.reset(RdKafka::Consumer::create(_conf.get(), errstr));
    uassert(ErrorCodes::UnknownError,
            str::stream() << "Failed to create consumer with error: " << errstr,
            _consumer);

    _topic.reset(
        RdKafka::Topic::create(_consumer.get(), _options.topicName, /*conf*/ nullptr, errstr));
    uassert(ErrorCodes::UnknownError,
            str::stream() << "Failed to create topic handle with error: " << errstr,
            _topic);
}

void KafkaPartitionConsumer::doStart() {
    RdKafka::ErrorCode resp =
        _consumer->start(_topic.get(), _options.partition, _options.startOffset);
    uassert(ErrorCodes::UnknownError,
            str::stream() << "Failed to start consumer with error: " << RdKafka::err2str(resp),
            resp == RdKafka::ERR_NO_ERROR);

    dassert(!_consumerThread.joinable());
    _consumerThread = stdx::thread([this] { fetchLoop(); });
}

KafkaPartitionConsumer::~KafkaPartitionConsumer() {
    // Make sure that stop() has already been called if necessary.
    dassert(!_consumerThread.joinable());
}

void KafkaPartitionConsumer::doStop() {
    // Stop the consumer thread.
    bool joinThread{false};
    if (_consumerThread.joinable()) {
        stdx::lock_guard<Latch> fLock(_finalizedDocBatch.mutex);
        joinThread = true;
        _finalizedDocBatch.shutdown = true;
        _consumerThreadWakeUpCond.notify_one();
    }
    if (joinThread) {
        // Wait for the consumer thread to exit.
        _consumerThread.join();
    }

    _consumer->stop(_topic.get(), _options.partition);
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
                _finalizedDocBatch.pushDocVec(_activeDocBatch.popDocVec());
            }
        }

        if (_finalizedDocBatch.exception) {
            // Throw the exception to the caller.
            std::rethrow_exception(_finalizedDocBatch.exception);
        }
        if (!_finalizedDocBatch.empty()) {
            dassert(!_finalizedDocBatch.docVecs.empty());
            docs = _finalizedDocBatch.popDocVec();
        }
        _consumerThreadWakeUpCond.notify_all();
    }
    return docs;
}

std::unique_ptr<RdKafka::Conf> KafkaPartitionConsumer::createKafkaConf() {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto setConf = [confPtr = conf.get()](const std::string& confName,
                                          const std::string& confValue) {
        std::string errstr;
        if (confPtr->set(confName, confValue, errstr) != RdKafka::Conf::CONF_OK) {
            uasserted(ErrorCodes::UnknownError,
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

    // Set auth related configurations.
    for (const auto& config : _options.authConfig) {
        setConf(config.first, config.second);
    }

    // TODO(sandeep): Set more config options that could be useful.
    return conf;
}

void KafkaPartitionConsumer::fetchLoop() {
    ConsumeCbImpl consumeCbImpl(this);
    int32_t numDocsToFetch{0};
    while (true) {
        {
            stdx::unique_lock fLock(_finalizedDocBatch.mutex);
            if (_finalizedDocBatch.shutdown) {
                LOGV2_INFO(
                    74681, "{partition}: exiting fetchLoop()", "partition"_attr = partition());
                break;
            }

            if (numDocsToFetch <= 0) {
                if (_finalizedDocBatch.size() < _options.maxNumDocsToPrefetch) {
                    numDocsToFetch = _options.maxNumDocsToPrefetch - _finalizedDocBatch.size();
                } else {
                    LOGV2_DEBUG(74678,
                                1,
                                "{partition}: sleeping when numDocs: {numDocs}"
                                " numDocsReturned: {numDocsReturned}",
                                "partition"_attr = partition(),
                                "numDocs"_attr = _finalizedDocBatch.numDocs,
                                "numDocsReturned"_attr = _finalizedDocBatch.numDocsReturned);
                    _consumerThreadWakeUpCond.wait(fLock, [this]() {
                        return _finalizedDocBatch.shutdown ||
                            _finalizedDocBatch.size() < _options.maxNumDocsToPrefetch;
                    });
                    LOGV2_DEBUG(74679,
                                1,
                                "{partition}: waking up when numDocs: {numDocs}"
                                " numDocsReturned: {numDocsReturned}",
                                "partition"_attr = partition(),
                                "numDocs"_attr = _finalizedDocBatch.numDocs,
                                "numDocsReturned"_attr = _finalizedDocBatch.numDocsReturned);
                }
            }
        }

        // TODO(sandeep): During local testing, I noticed that numDocsFetched
        // can be higher than actual number of docs fetched. Does it matter? Can we fix it?
        // TODO(sandeep): Test that it is ok to not call consume_callback() for an extended
        // period of time.
        int numDocsFetched =
            _consumer->consume_callback(_topic.get(),
                                        _options.partition,
                                        _options.kafkaOptions.kafkaConsumeCallbackTimeoutMs,
                                        &consumeCbImpl,
                                        /*opaque*/ nullptr);
        LOGV2_DEBUG(74680,
                    1,
                    "{partition}: numDocsFetched by consume_callback(): {numDocsFetched}",
                    "partition"_attr = partition(),
                    "numDocsFetched"_attr = numDocsFetched);
        numDocsToFetch -= numDocsFetched;
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
            _activeDocBatch.emplaceDocVec(getMaxDocVecSize());
        }

        // Assert that there is capacity available in the last DocVec.
        dassert(_activeDocBatch.docVecs.back().size() < getMaxDocVecSize());
        _activeDocBatch.pushDocToLastDocVec(std::move(doc));

        if (_activeDocBatch.docVecs.back().size() == getMaxDocVecSize()) {
            _activeDocBatch.emplaceDocVec(getMaxDocVecSize());
        }
        numActiveDocVecs = _activeDocBatch.docVecs.size();
    }

    if (numActiveDocVecs > 1) {
        // Move all full DocVecs from _activeDocBatch to _finalizedDocBatch.
        // Note that we released _activeDocBatch.mutex above, so the state of _activeDocBatch
        // could now be different.
        stdx::lock_guard<Latch> fLock(_finalizedDocBatch.mutex);
        stdx::lock_guard<Latch> aLock(_activeDocBatch.mutex);
        while (!_activeDocBatch.docVecs.empty()) {
            if (_activeDocBatch.docVecs.front().size() < getMaxDocVecSize()) {
                // Only the last DocVec may have capacity available in it.
                dassert(_activeDocBatch.docVecs.size() == 1);
                break;
            }
            auto docs = _activeDocBatch.popDocVec();
            _finalizedDocBatch.pushDocVec(std::move(docs));
        }
    }
}

void KafkaPartitionConsumer::onMessage(const RdKafka::Message& message) {
    switch (message.err()) {
        case RdKafka::ERR__TIMED_OUT:
            break;  // Do nothing.
        case RdKafka::ERR_NO_ERROR: {
            pushDocToActiveDocBatch(processMessagePayload(message));
            break;
        }
        default: {
            uasserted(ErrorCodes::UnknownError,
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

KafkaSourceDocument KafkaPartitionConsumer::processMessagePayload(const RdKafka::Message& message) {
    dassert(message.err() == RdKafka::ERR_NO_ERROR);

    KafkaSourceDocument sourceDoc;
    const char* buf = static_cast<const char*>(message.payload());
    try {
        sourceDoc.doc = _options.deserializer->deserialize(buf, message.len());
    } catch (const std::exception& e) {
        LOGV2_ERROR(74682,
                    "{partition}: Failed to parse input message: {error}",
                    "partition"_attr = partition(),
                    "error"_attr = e.what());
        sourceDoc.doc = boost::none;
        sourceDoc.error = str::stream() << "Failed to parse input message with error:" << e.what();
    }

    sourceDoc.partition = partition();
    sourceDoc.offset = message.offset();
    sourceDoc.sizeBytes = message.len();
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
