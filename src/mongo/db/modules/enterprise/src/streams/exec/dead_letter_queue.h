#pragma once

#include <string>
#include <vector>

#include "streams/exec/message.h"

namespace streams {

/**
 * The abstract base class of all dead letter queue implementations.
 */
class DeadLetterQueue {
public:
    virtual ~DeadLetterQueue() = default;

    // TODO: If a failure happens in some other part of the pipeline (e.g. in $replaceRoot),
    // we would still want to add the event to the dead letter queue. This means that we need to
    // change this interface as we would not have a KafkaSourceDocument at that point.
    virtual void addMessage(KafkaSourceDocument msg) {
        return doAddMessage(std::move(msg));
    }

protected:
    virtual void doAddMessage(KafkaSourceDocument msg) = 0;
};

}  // namespace streams
