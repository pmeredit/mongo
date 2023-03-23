#pragma once

#include <string>
#include <vector>

#include "streams/exec/message.h"

namespace streams {

/**
 * The abstract base class of classes used to tail documents from a partition of a Kafka topic.
 */
class KafkaPartitionConsumerBase {
public:
    virtual ~KafkaPartitionConsumerBase() = default;

    // Initializes internal state.
    // Throws an exception if any error is encountered during the initialization.
    virtual void init() {
        doInit();
    }

    // Starts the consumer.
    virtual void start() {
        doStart();
    }

    // Stops the consumer.
    virtual void stop() {
        doStop();
    }

    // Returns the next batch of documents tailed from the partition, if any available.
    virtual std::vector<KafkaSourceDocument> getDocuments() {
        return doGetDocuments();
    }

protected:
    virtual void doInit() = 0;

    virtual void doStart() = 0;

    virtual void doStop() = 0;

    virtual std::vector<KafkaSourceDocument> doGetDocuments() = 0;
};

}  // namespace streams
