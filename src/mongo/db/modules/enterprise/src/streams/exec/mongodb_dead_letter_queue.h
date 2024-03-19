#pragma once

#include "streams/util/metric_manager.h"
#include <memory>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/producer_consumer_queue.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/util/metrics.h"

namespace streams {

struct Context;

/**
 * MongoDBDeadLetterQueue implements the DeadLetterQueue interface
 * using a remote MongoDB collection. The mongocxx driver is used to
 * connect to the remote MongoDB.
 */
class MongoDBDeadLetterQueue : public DeadLetterQueue {
public:
    MongoDBDeadLetterQueue(Context* context, MongoCxxClientOptions options);
    void registerMetrics(MetricManager* executor) override;

private:
    // Single queue entry, only either `data` or `flushSignal` will be set. The
    // `flushSignal` is only used internally on `flush()` to ensure that the
    // consumer thread signals back to the caller thread that the queue has been
    // flushed to mongodb.
    struct Message {
        // Document received from `doAddMessage`, this is only marked as optional for
        // the case where `flushSignal` is set, which is used internally for `flush()`.
        boost::optional<bsoncxx::document::value> data;

        // Used by `doFlush()` to have the consumer thread signal back to the main
        // thread that the inflight document batch has been written out to mongodb.
        bool flushSignal{false};
    };

    // Cost function for the queue so that we limit the max queue size based on the
    // byte size of the documents rather than having the same weight for each document.
    struct QueueCostFunc {
        size_t operator()(const Message& msg) const {
            if (!msg.data) {
                // This Is only the case for internal `flush()` messages.
                return 1;
            }

            return msg.data.get().length();
        }
    };

    // Inserts the message into a queue, the message is then asynchronously written to
    // mongodb from a separate consumer thread.
    int doAddMessage(mongo::BSONObj msg) override;

    // Spawns the consumer thread that will consume documents from the queue and write
    // them into mongodb.
    void doStart() override;

    // Shuts down the consumer thread, it does not wait for the queue to be fully flushed,
    // so any pending documents within the queue will be dropped when stop is called.
    void doStop() override;

    // Waits for the queue to be fully drained and for all pending documents to be flushed
    // to mongodb.
    void doFlush() override;

    SPStatus doGetStatus() override;

    void consumeLoop();

    const MongoCxxClientOptions _options;
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri;
    std::unique_ptr<mongocxx::client> _client;
    std::unique_ptr<mongocxx::database> _database;
    std::unique_ptr<mongocxx::collection> _collection;
    mongocxx::options::insert _insertOptions;

    std::shared_ptr<Counter> _dlqErrorsCounter;
    std::shared_ptr<CallbackGauge> _queueSize;

    // All messages are processed asynchronously by the `_consumerThread`. We want this to be
    // multi-producer rather than single-producer because some operators have background threads
    // that may add documents to the DLQ.
    mongo::MultiProducerSingleConsumerQueue<Message, QueueCostFunc> _queue;
    mongo::stdx::thread _consumerThread;
    mutable mongo::Mutex _consumerMutex =
        MONGO_MAKE_LATCH("MongoDBDeadLetterQueue::_consumerMutex");
    SPStatus _consumerStatus{mongo::Status::OK()};
    bool _pendingFlush{false};
    mongo::stdx::condition_variable _flushedCv;
    bool _consumerThreadRunning{false};
};

}  // namespace streams
