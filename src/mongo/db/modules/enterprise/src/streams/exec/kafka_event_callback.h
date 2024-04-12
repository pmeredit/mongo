#pragma once

#include "mongo/platform/mutex.h"
#include "streams/util/exception.h"

#include <deque>
#include <rdkafka.h>
#include <rdkafkacpp.h>

namespace streams {

struct Context;

// KafkaEventCallback is used to interpret and log messages from librdkafka.
class KafkaEventCallback : public RdKafka::EventCb {
public:
    KafkaEventCallback(Context* context, std::string operatorName)
        : _context(context), _operatorName(std::move(operatorName)) {}

    // Called by librdkafka.
    void event_cb(RdKafka::Event& event) override;

    // Returns true if the Kafka connection has an error.
    bool hasError();

    // Appends messages from librdkafka to the status reason.
    SPStatus appendRecentErrorsToStatus(SPStatus status);

private:
    // Process an error event from kafka.
    void processErrorEvent(RdKafka::Event& event);

    Context* _context{nullptr};
    std::string _operatorName;

    // Protects access to the error messages buffer.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("KafkaErrorBuffer::mutex");
    std::deque<std::string> _errorBuffer;
    // Set to true when a librdkafka indicates we should error out.
    bool _hasError{false};
};


}  // namespace streams
