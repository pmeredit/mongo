/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/kafka_event_callback.h"

#include <boost/algorithm/string/join.hpp>
#include <rdkafkacpp.h>

#include "mongo/bson/bsontypes.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

namespace {

BSONObj toBSON(const RdKafka::Event& event) {
    return BSON("type" << event.type() << "msg" << event.str() << "err" << event.err() << "errStr"
                       << RdKafka::err2str(event.err()) << "severity" << event.severity());
}

constexpr static int kMaxErrorBufferSize{5};

}  // namespace

void KafkaEventCallback::processErrorEvent(RdKafka::Event& event) {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    if (event.fatal()) {
        _hasError = true;
    }
    if (_errorBuffer.size() == kMaxErrorBufferSize) {
        _errorBuffer.pop_front();
    }
    _errorBuffer.push_back(event.str());
}

bool KafkaEventCallback::hasError() {
    stdx::unique_lock<stdx::mutex> lock(_mutex);
    return _hasError;
}

SPStatus KafkaEventCallback::appendRecentErrorsToStatus(SPStatus status) {
    if (!hasError()) {
        // If there are no errors, we should wait briefly to see if any new errors are appended from
        // the async callback
        sleepFor(Seconds{2});
    }
    std::deque<std::string> kafkaMessages;
    {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        std::swap(_errorBuffer, kafkaMessages);
    }

    kafkaMessages.push_front(status.reason());
    std::string detailedMsg = boost::algorithm::join(kafkaMessages, ", ");
    return SPStatus{Status{status.code(), std::move(detailedMsg)}, status.unsafeReason()};
}

void KafkaEventCallback::event_cb(RdKafka::Event& event) {
    switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR: {
            LOGV2_ERROR(76441,
                        "Kafka error event",
                        "context"_attr = _context,
                        "event"_attr = toBSON(event));
            processErrorEvent(event);
            break;
        };
        case RdKafka::Event::EVENT_STATS: {
            LOGV2_DEBUG(76439,
                        2,
                        "Kafka stats event",
                        "event"_attr = toBSON(event),
                        "operatorName"_attr = _operatorName,
                        "context"_attr = _context);
            break;
        }
        case RdKafka::Event::EVENT_LOG: {
            auto sev = event.severity();
            if (sev == RdKafka::Event::EVENT_SEVERITY_EMERG ||
                sev == RdKafka::Event::EVENT_SEVERITY_ALERT ||
                sev == RdKafka::Event::EVENT_SEVERITY_CRITICAL ||
                sev == RdKafka::Event::EVENT_SEVERITY_ERROR) {
                LOGV2_INFO(76440,
                           "Kafka error log event",
                           "context"_attr = _context,
                           "operatorName"_attr = _operatorName,
                           "event"_attr = toBSON(event));
                processErrorEvent(event);
            } else if (sev == RdKafka::Event::EVENT_SEVERITY_WARNING) {
                LOGV2_INFO(76446,
                           "Kafka warning log event",
                           "context"_attr = _context,
                           "operatorName"_attr = _operatorName,
                           "event"_attr = toBSON(event));
            } else if (sev == RdKafka::Event::EVENT_SEVERITY_DEBUG) {
                LOGV2_INFO(9841301,
                           "Kafka debug log event",
                           "context"_attr = _context,
                           "operatorName"_attr = _operatorName,
                           "event"_attr = toBSON(event));
            }
            break;
        }
        case RdKafka::Event::EVENT_THROTTLE:
            LOGV2_INFO(76442,
                       "Kafka throttle event",
                       "event"_attr = toBSON(event),
                       "operatorName"_attr = _operatorName,
                       "context"_attr = _context);
            break;
        default:
            LOGV2_WARNING(76443,
                          "Kafka unknown event",
                          "context"_attr = _context,
                          "operatorName"_attr = _operatorName,
                          "event"_attr = toBSON(event));
            break;
    }
}

}  // namespace streams
