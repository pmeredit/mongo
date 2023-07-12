#include "streams/exec/log_sink_operator.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

void LogSinkOperator::doSinkOnDataMsg(int32_t inputIdx,
                                      StreamDataMsg dataMsg,
                                      boost::optional<StreamControlMsg> controlMsg) {
    for (auto& doc : dataMsg.docs) {
        LOGV2_INFO(5739600, "data", "doc"_attr = doc.doc.toString());
    }

    if (controlMsg) {
        logControl(controlMsg.value());
    }
}

void LogSinkOperator::doSinkOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    logControl(controlMsg);
}

void LogSinkOperator::logControl(StreamControlMsg controlMsg) {
    int64_t watermarkTime = controlMsg.watermarkMsg.value().eventTimeWatermarkMs;
    auto watermarkStatus = controlMsg.watermarkMsg.value().watermarkStatus;
    LOGV2_INFO(5739601,
               "control",
               "watermarkTime"_attr = watermarkTime,
               "watermarkStatus"_attr = watermarkStatus);
}

}  // namespace streams
