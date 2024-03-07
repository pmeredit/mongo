#pragma once

#include <memory>
#include <string>

#include "mongo/base/status.h"
#include "streams/exec/output_sampler.h"
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

    // Add the BSON object to the dead letter queue and returns the bytes added
    virtual int addMessage(mongo::BSONObjBuilder objBuilder);
    void start();

    // Shuts down the dead letter queue, this will not wait for any pending documents
    // that are being processed asynchronously to be fully flushed.
    void stop();

    // Waits for the queue to be fully drained and for all pending documents to be flushed.
    void flush();

    // Returns the status of this dead letter queue. If the status is not okay, then
    // that means that the dead letter queue has stopped and is no longer accepting messages
    // and that the stream processor should error out.
    mongo::Status getStatus();

    virtual void registerMetrics(MetricManager* executor);

    // Add an output sampler.
    void addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler);

protected:
    virtual int doAddMessage(mongo::BSONObj msg) = 0;

    virtual void doStart() {}
    virtual void doStop() {}
    virtual void doFlush() {}
    virtual mongo::Status doGetStatus() {
        return mongo::Status::OK();
    }

    // Send output to the list of samplers.
    void sendOutputToSamplers(const mongo::BSONObj& msg);

    Context* _context{nullptr};
    // Exports number of documents added to the dead letter queue.
    std::shared_ptr<Counter> _numDlqDocumentsCounter;
    // Number of bytes sent to dead letter queue
    std::shared_ptr<Counter> _numDlqBytesCounter;

    // Mutex used to protect the member below.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("DeadLetterQueue::mutex");

    // Current output samplers.
    std::vector<boost::intrusive_ptr<OutputSampler>> _outputSamplers;
};

}  // namespace streams
