#pragma once

#include <string>

#include "mongo/db/namespace_string.h"

namespace streams {

/**
 * The abstract base class of all dead letter queue implementations.
 */
class DeadLetterQueue {
public:
    DeadLetterQueue(mongo::NamespaceString ns);

    virtual ~DeadLetterQueue() = default;

    virtual void addMessage(mongo::BSONObjBuilder objBuilder);

protected:
    virtual void doAddMessage(mongo::BSONObj msg) = 0;

    mongo::NamespaceString _ns;
};

}  // namespace streams
