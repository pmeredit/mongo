#pragma once

#include <rdkafka.h>
#include <rdkafkacpp.h>

namespace streams {

struct Context;

// KafkaEventCallback is used to emit logs from librdkafka.
class KafkaEventCallback : public RdKafka::EventCb {
public:
    KafkaEventCallback(Context* context, std::string operatorName)
        : _context(context), _operatorName(std::move(operatorName)) {}
    // Called by librdkafka.
    void event_cb(RdKafka::Event& event) override;

private:
    Context* _context{nullptr};
    std::string _operatorName;
};


}  // namespace streams
