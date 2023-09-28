/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <chrono>

#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_dead_letter_queue.h"
#include "streams/util/metric_manager.h"

namespace streams {

using namespace mongo;

namespace {

// Max size of the queue (in bytes) until `push()` starts blocking.
static constexpr int64_t kQueueMaxSizeBytes = 128 * 1024 * 1024;  // 128 MB

// Max size of each write batch (in bytes) to mongodb.
static constexpr int64_t kWriteBatchMaxSizeBytes = 1 * 1024 * 1024;  // 1 MB

};  // namespace

MongoDBDeadLetterQueue::MongoDBDeadLetterQueue(Context* context,
                                               streams::MongoCxxClientOptions options)
    : DeadLetterQueue(context),
      _options(options),
      _queue(decltype(_queue)::Options{.maxQueueDepth = kQueueMaxSizeBytes}) {
    _instance = getMongocxxInstance(_options.svcCtx);
    _uri = std::make_unique<mongocxx::uri>(_options.uri);
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

    MetricManager::LabelsVec labels = getDefaultMetricLabels(_context);
    labels.push_back(std::make_pair("kind", "mongodb"));
    _dlqErrorsCounter = _context->metricManager->registerCounter(
        "dlq_errors", "Number of errors encountered when writing to the dead letter queue", labels);
}

void MongoDBDeadLetterQueue::doAddMessage(BSONObj msg) {
    _queue.push(Message{.data = toBsoncxxDocument(msg)});
}

void MongoDBDeadLetterQueue::doStart() {
    dassert(!_consumerThread.joinable());
    _consumerThread = stdx::thread([this]() { consumeLoop(); });
}

void MongoDBDeadLetterQueue::doStop() {
    dassert(_consumerThread.joinable());

    // This will close the queue which will make the consumer thread exit as well
    // because this will trigger a `ProducerConsumerQueueConsumed` exception in the
    // consumer thread.
    _queue.closeProducerEnd();
    _consumerThread.join();
}

boost::optional<std::string> MongoDBDeadLetterQueue::doGetError() {
    stdx::lock_guard<Latch> lock(_consumerMutex);
    return _consumerError;
}

void MongoDBDeadLetterQueue::doFlush() {
    // Wait until all the messages in the queue have been consumed and inserted into mongodb.
    stdx::unique_lock<Latch> lock(_consumerMutex);
    _queue.push(Message{.flushSignal = true});
    _flushedCv.wait(lock);
}

void MongoDBDeadLetterQueue::consumeLoop() {
    bool success{true};
    while (success) {
        try {
            auto [batch, _] = _queue.popManyUpTo(kWriteBatchMaxSizeBytes);

            std::vector<bsoncxx::document::value> docBatch;
            bool flushSignal{false};
            for (auto& msg : batch) {
                if (msg.flushSignal) {
                    dassert(!msg.data);
                    flushSignal = true;
                } else {
                    dassert(msg.data);
                    docBatch.push_back(std::move(*msg.data));
                }
            }

            // The document batch may be empty if the queue was empty and we only
            // received a flush signal.
            if (!docBatch.empty()) {
                auto result = _collection->insert_many(std::move(docBatch), _insertOptions);
                if (!result) {
                    success = false;
                }
            }

            stdx::lock_guard<Latch> lock(_consumerMutex);
            if (flushSignal) {
                _flushedCv.notify_all();
            }

            if (!success) {
                _dlqErrorsCounter->increment();
                _consumerError = "dlq mongodb insert failed";
            }
        } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueConsumed>&) {
            // Closed naturally from `stop()`.
            success = false;
        } catch (const std::exception& ex) {
            stdx::lock_guard<Latch> lock(_consumerMutex);
            _consumerError = ex.what();
            success = false;
        }
    }
}

}  // namespace streams
