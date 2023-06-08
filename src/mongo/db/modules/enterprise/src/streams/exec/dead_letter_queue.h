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

/**
 * The abstract base class of all dead letter queue implementations.
 */
class DeadLetterQueue {
public:
    DeadLetterQueue(Context* context);

    virtual ~DeadLetterQueue() = default;

    virtual void addMessage(mongo::BSONObjBuilder objBuilder);

protected:
    virtual void doAddMessage(mongo::BSONObj msg) = 0;

    Context* _context{nullptr};
    // Exports number of documents added to the dead letter queue.
    std::shared_ptr<Counter> _numDlqDocumentsCounter;
};

}  // namespace streams
