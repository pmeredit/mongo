#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/name_expression.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/thread.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/producer_consumer_queue.h"
#include "streams/exec/sink_operator.h"

namespace mongo {
class DocumentSourceMerge;
class MergeProcessor;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The operator for $merge.
 */
// TODO: DocumentSourceMerge does internal buffering. We may want to explicitly flush in some cases
// or have a timeout on this buffering.
class MergeOperator : public SinkOperator {
public:
    struct Options {
        // DocumentSource stage that this Operator wraps.
        mongo::DocumentSourceMerge* documentSource;
        mongo::NameExpression db;
        mongo::NameExpression coll;
    };

    MergeOperator(Context* context, Options options);

    mongo::DocumentSourceMerge* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "MergeOperator";
    }

    void doSinkOnDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) override;

private:
    // Single queue entry, only either `data` or `flushSignal` will be set. The
    // `flushSignal` is only used internally on `flush()` to ensure that the
    // consumer thread signals back to the caller thread that the queue has been
    // flushed to mongodb before commiting the checkpointing.
    struct Message {
        // Document received from `doSinkOnDataMsg`, this is only marked as optional for
        // the case where `flushSignal` is set, which is used internally for `flush()`.
        boost::optional<StreamDataMsg> data;

        // Used by checkpointing to ensure that the queue is drained and that the inflight
        // document batch has been written out to mongodb.
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

            return msg.data.get().getSizeBytes();
        }
    };

    void doStart() override;
    void doStop() override;
    void doFlush() override;
    boost::optional<std::string> doGetError() override;
    OperatorStats doGetStats() override;

    using NsKey = std::pair<std::string, std::string>;
    using DocIndices = std::vector<size_t>;
    using DocPartitions = mongo::stdx::unordered_map<NsKey, DocIndices>;

    DocPartitions partitionDocsByTargets(const StreamDataMsg& dataMsg);

    // Processes the docs at the indexes in 'docIndices' each of which points to a doc in 'dataMsg'.
    // This returns the stats for the batch of messages passed in.
    OperatorStats processStreamDocs(const StreamDataMsg& dataMsg,
                                    const mongo::NamespaceString& outputNs,
                                    const std::vector<size_t>& docIndices,
                                    size_t maxBatchDocSize);

    // Loop for the `_consumerThread` that is responsible for asynchronously writing out
    // documents to mongodb.
    void consumeLoop();

    Options _options;
    mongo::MergeProcessor* _processor{nullptr};

    // All messages are processed asynchronously by the `_consumerThread`.
    mongo::SingleProducerSingleConsumerQueue<Message, QueueCostFunc> _queue;
    mongo::stdx::thread _consumerThread;
    mutable mongo::Mutex _consumerMutex = MONGO_MAKE_LATCH("MergeOperator::_consumerMutex");
    boost::optional<std::string> _consumerError;
    bool _consumerThreadRunning{false};
    bool _pendingFlush{false};
    mongo::stdx::condition_variable _flushedCv;
    std::shared_ptr<CallbackGauge> _queueSize;

    // Stats tracked by the consumer thread. Write and read access to these stats must be
    // protected by `_consumerMutex`. This will be merged with the root level `_stats`
    // when `doGetStats()` is called.
    OperatorStats _consumerStats;
};

}  // namespace streams
