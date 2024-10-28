/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <chrono>

#include "mongo/base/status.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_dead_letter_queue.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/util/metric_manager.h"
#include <exception>
#include <mongocxx/exception/exception.hpp>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

namespace {

// Max size of each write batch (in bytes) to mongodb.
static constexpr int64_t kWriteBatchMaxSizeBytes = 1 * 1024 * 1024;  // 1 MB

};  // namespace

MongoDBDeadLetterQueue::MongoDBDeadLetterQueue(Context* context,
                                               streams::MongoCxxClientOptions options)
    : DeadLetterQueue(context),
      _options(options),
      _queue(decltype(_queue)::Options{
          .maxQueueDepth = static_cast<size_t>(getMaxSinkQueueSizeBytes(_context->featureFlags))}) {

    _instance = getMongocxxInstance(_options.svcCtx);
    _uri = makeMongocxxUri(_options.uri);
    _client = std::make_unique<mongocxx::client>(*_uri, _options.toMongoCxxClientOptions());
    tassert(8143700, "Expected database name but got none", _options.database);
    _database = std::make_unique<mongocxx::database>(_client->database(*_options.database));
    tassert(8143701, "Expected collection name but got none", _options.collection);
    _collection =
        std::make_unique<mongocxx::collection>(_database->collection(*_options.collection));

    mongocxx::write_concern writeConcern;
    writeConcern.journal(true);
    writeConcern.acknowledge_level(mongocxx::write_concern::level::k_majority);
    // TODO(SERVER-76564): Handle timeouts, adjust this value.
    writeConcern.majority(/*timeout*/ stdx::chrono::milliseconds(60 * 1000));
    _insertOptions = mongocxx::options::insert().write_concern(std::move(writeConcern));

    _errorPrefix =
        fmt::format("Dead letter queue {}.{} failed", *_options.database, *_options.collection);
}

int MongoDBDeadLetterQueue::doAddMessage(BSONObj msg) {
    auto objSize = msg.objsize();
    _queue.push(Message{.data = toBsoncxxValue(msg)});

    return objSize;
}

void MongoDBDeadLetterQueue::registerMetrics(MetricManager* metricManager) {
    MetricManager::LabelsVec labels = getDefaultMetricLabels(_context);

    DeadLetterQueue::registerMetrics(metricManager);
    labels.push_back(std::make_pair("kind", "mongodb"));
    _dlqErrorsCounter = metricManager->registerCounter(
        "dlq_errors", "Number of errors encountered when writing to the dead letter queue", labels);
    _queueSize = metricManager->registerCallbackGauge(
        /* name */ "dlq_queue_bytesize",
        /* description */ "Total bytes currently buffered in the queue",
        /* labels */ labels,
        [this]() { return _queue.getStats().queueDepth; });
}

void MongoDBDeadLetterQueue::doStart() {
    stdx::unique_lock<stdx::mutex> lock(_consumerMutex);
    dassert(!_consumerThread.joinable());
    dassert(!_consumerThreadRunning);
    _consumerThread = stdx::thread([this]() {
        auto status = runMongocxxNoThrow([this]() { callHello(*_database); },
                                         _context,
                                         ErrorCodes::Error{8191500},
                                         _errorPrefix,
                                         *_uri);

        if (!status.isOK()) {
            // Error connecting, quit early.
            stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
            _consumerThreadRunning = false;
            _consumerStatus = std::move(status);
            _flushedCv.notify_all();
            return;
        }

        consumeLoop();
    });
    _consumerThreadRunning = true;
}

void MongoDBDeadLetterQueue::doStop() {
    // This will close the queue which will make the consumer thread exit as well
    // because this will trigger a `ProducerConsumerQueueConsumed` exception in the
    // consumer thread.
    _queue.closeConsumerEnd();
    if (_consumerThread.joinable()) {
        _consumerThread.join();
    }
}

SPStatus MongoDBDeadLetterQueue::doGetStatus() {
    stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
    return _consumerStatus;
}

void MongoDBDeadLetterQueue::doFlush() {
    // Wait until all the messages in the queue have been consumed and inserted into mongodb.
    stdx::unique_lock<stdx::mutex> lock(_consumerMutex);

    dassert(!_pendingFlush);
    _pendingFlush = true;
    _queue.push(Message{.flushSignal = true});
    _flushedCv.wait(lock, [this]() -> bool { return !_consumerThreadRunning || !_pendingFlush; });

    // Make sure that an error wasn't encountered in the background consumer thread while
    // waiting for the flushed condvar to be notified.
    uassert(75387,
            str::stream() << "unable to flush mongodb DLQ with error: " << _consumerStatus.reason(),
            _consumerStatus.isOK() && !_pendingFlush);
}

void MongoDBDeadLetterQueue::consumeLoop() {
    bool done{false};
    SPStatus status{Status::OK()};

    while (!done) {
        try {
            std::vector<bsoncxx::document::value> docBatch;
            bool flushSignal{false};
            int accumulatedBytes{0};
            boost::optional<Message> msg;
            // Here the kWriteBatchMaxSizeBytes is a soft limit on the number of bytes accumulated
            // in the docBatch. We exit the loop when either of the following conditions are met
            //      - the number of bytes in the docBatch is more than the soft limit of
            //        kWriteBatchMaxSizeBytes,
            //      - no more messages in the queue
            _queue.waitForNonEmpty(Interruptible::notInterruptible());
            while (accumulatedBytes < kWriteBatchMaxSizeBytes && (msg = _queue.tryPop())) {
                if (msg->flushSignal) {
                    dassert(!msg->data);
                    flushSignal = true;
                } else {
                    dassert(msg->data);
                    accumulatedBytes += msg->data->length();
                    docBatch.push_back(std::move(*msg->data));
                }
            }

            // The document batch may be empty if the queue was empty and we only
            // received a flush signal.
            if (!docBatch.empty()) {
                auto result = _collection->insert_many(std::move(docBatch), _insertOptions);
                if (!result) {
                    done = true;
                    status = Status{ErrorCodes::Error{8191505}, "insert failed in DLQ"};
                }
            }

            if (status.isOK() && flushSignal) {
                stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
                _pendingFlush = false;
                _flushedCv.notify_all();
            }
        } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>&) {
            // Closed naturally from `stop()`.
            done = true;
        } catch (const mongocxx::exception& e) {
            status = mongocxxExceptionToStatus(e, *_uri, _errorPrefix);
            done = true;
        } catch (const std::exception& ex) {
            LOGV2_ERROR(8112612,
                        "Error encountered while writing to the DLQ.",
                        "context"_attr = _context,
                        "exception"_attr = ex.what());
            status = Status{
                ErrorCodes::Error{8191506},
                fmt::format("Error encountered while writing to the DLQ with db: {}, coll: {}",
                            _options.database ? *_options.database : "",
                            _options.collection ? *_options.collection : "")};
            done = true;
        }
    }

    // This will cause any thread calling _queue.push to throw an exception.
    _queue.closeConsumerEnd();

    // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
    // loop because of an exception, then the flush in the executor thread will fail after
    // it receives the flushed condvar signal.
    stdx::lock_guard<stdx::mutex> lock(_consumerMutex);
    _consumerThreadRunning = false;
    _consumerStatus = std::move(status);
    if (!_consumerStatus.isOK()) {
        _dlqErrorsCounter->increment();
    }

    _flushedCv.notify_all();
}

}  // namespace streams
