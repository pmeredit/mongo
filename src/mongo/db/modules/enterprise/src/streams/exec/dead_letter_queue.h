#pragma once

#include <memory>
#include <string>

#include "streams/util/metrics.h"

namespace mongo {
class BSONObj;
class BSONObjBuilder;
}  // namespace mongo

namespace streams {

struct Context;
class MetricManager;

/**
 * The abstract base class of all dead letter queue implementations.
 */
class DeadLetterQueue {
public:
    DeadLetterQueue(Context* context);

    virtual ~DeadLetterQueue() = default;

    virtual void addMessage(mongo::BSONObjBuilder objBuilder);
    void start();

    // Shuts down the dead letter queue, this will not wait for any pending documents
    // that are being processed asynchronously to be fully flushed.
    void stop();

    // Waits for the queue to be fully drained and for all pending documents to be flushed.
    void flush();

    // Returns the last seen error for this dead letter queue. If the error here is set, then
    // that means that the dead letter queue has stopped and is no longer accepting messages
    // and that the stream processor should error out.
    boost::optional<std::string> getError();

    virtual void registerMetrics(MetricManager* executor);

protected:
    virtual void doAddMessage(mongo::BSONObj msg) = 0;

    virtual void doStart() {}
    virtual void doStop() {}
    virtual void doFlush() {}
    virtual boost::optional<std::string> doGetError() {
        return boost::none;
    }

    Context* _context{nullptr};
    // Exports number of documents added to the dead letter queue.
    std::shared_ptr<Counter> _numDlqDocumentsCounter;
};

}  // namespace streams
