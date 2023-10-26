/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/kafka_event_callback.h"

#include "streams/exec/context.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

void KafkaEventCallback::event_cb(RdKafka::Event& event) {
    switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            LOGV2_ERROR(76441,
                        "Kafka error event",
                        "context"_attr = _context,
                        "errorStr"_attr = RdKafka::err2str(event.err()),
                        "operatorName"_attr = _operatorName,
                        "event"_attr = event.str());
            break;
        case RdKafka::Event::EVENT_STATS:
            LOGV2_DEBUG(76439,
                        2,
                        "Kafka stats event",
                        "event"_attr = event.str(),
                        "operatorName"_attr = _operatorName,
                        "context"_attr = _context);
            break;
        case RdKafka::Event::EVENT_LOG: {
            auto sev = event.severity();
            if (sev == RdKafka::Event::EVENT_SEVERITY_EMERG ||
                sev == RdKafka::Event::EVENT_SEVERITY_ALERT ||
                sev == RdKafka::Event::EVENT_SEVERITY_CRITICAL ||
                sev == RdKafka::Event::EVENT_SEVERITY_ERROR) {
                LOGV2_ERROR(76440,
                            "Kafka error log event",
                            "context"_attr = _context,
                            "severity"_attr = sev,
                            "operatorName"_attr = _operatorName,
                            "event"_attr = event.str());
            } else if (sev == RdKafka::Event::EVENT_SEVERITY_WARNING) {
                LOGV2_WARNING(76446,
                              "Kafka warning log event",
                              "context"_attr = _context,
                              "severity"_attr = sev,
                              "operatorName"_attr = _operatorName,
                              "event"_attr = event.str());
            }
            break;
        }
        case RdKafka::Event::EVENT_THROTTLE:
            LOGV2_WARNING(76442,
                          "Kafka throttle event",
                          "event"_attr = event.str(),
                          "operatorName"_attr = _operatorName,
                          "context"_attr = _context);
            break;
        default:
            LOGV2_WARNING(76443,
                          "Kafka unknown event",
                          "type"_attr = event.type(),
                          "context"_attr = _context,
                          "errorStr"_attr = RdKafka::err2str(event.err()),
                          "operatorName"_attr = _operatorName,
                          "event"_attr = event.str());
            break;
    }
}

}  // namespace streams
