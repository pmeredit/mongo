/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/operator.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

Operator::Operator(Context* context, int32_t numInputs, int32_t numOutputs)
    : _context(context),
      _numInputs(numInputs),
      _numOutputs(numOutputs),
      _memoryUsageHandle(context->memoryAggregator->createUsageHandle()) {
    _outputs.reserve(_numOutputs);
    _operatorTimer.pause();
}

void Operator::addOutput(Operator* oper, int32_t operInputIdx) {
    dassert(_outputs.size() < size_t(_numOutputs));
    _outputs.push_back(OutputInfo{oper, operInputIdx});
}

void Operator::start() {
    uassert(ErrorCodes::InternalError,
            str::stream() << getName() << " has " << _outputs.size() << " outputs, but "
                          << _numOutputs << " outputs are expected",
            _outputs.size() == size_t(_numOutputs));
    _stats.operatorName = getName();

    doStart();
}

void Operator::stop() {
    doStop();
}

std::string Operator::getName() const {
    return doGetName();
}

void Operator::onDataMsg(int32_t inputIdx,
                         StreamDataMsg dataMsg,
                         boost::optional<StreamControlMsg> controlMsg) {
    dassert(inputIdx < _numInputs);
    tassert(8183600, "Empty input message", !dataMsg.docs.empty());

    // _operatorTimer might already be running e.g. when onDataMsg() is called by onControlMsg().
    bool timerOriginallyRunning = _operatorTimer.isRunning();
    if (!timerOriginallyRunning) {
        _operatorTimer.unpause();
    }
    ScopeGuard guard([&] {
        if (!timerOriginallyRunning) {
            _operatorTimer.pause();
        }
    });

    OperatorStats stats;
    stats.numInputDocs += dataMsg.docs.size();
    if (shouldComputeInputByteStats()) {
        for (const auto& doc : dataMsg.docs) {
            stats.numInputBytes += doc.doc.getCurrentApproximateSize();
        }
    }
    incOperatorStats(std::move(stats));

    doOnDataMsg(inputIdx, std::move(dataMsg), std::move(controlMsg));
}

void Operator::onControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    // _operatorTimer might already be running e.g. when onControlMsg() is called by onDataMsg().
    bool timerOriginallyRunning = _operatorTimer.isRunning();
    if (!timerOriginallyRunning) {
        _operatorTimer.unpause();
    }
    ScopeGuard guard([&] {
        if (!timerOriginallyRunning) {
            _operatorTimer.pause();
        }
    });

    if (isSource()) {
        // For a $source, inputIdx == 0 is used to for checkpoint messages.
        invariant(inputIdx == 0);
    } else {
        invariant(inputIdx < _numInputs);
    }

    if (controlMsg.checkpointMsg) {
        invariant(_context->checkpointStorage);
    }

    doOnControlMsg(inputIdx, std::move(controlMsg));
}

OperatorStats Operator::getStats() {
    auto stats = doGetStats();
    stats.executionTime = _operatorTimer.elapsed();
    return stats;
}

void Operator::sendDataMsg(int32_t outputIdx,
                           StreamDataMsg dataMsg,
                           boost::optional<StreamControlMsg> controlMsg) {
    dassert(size_t(outputIdx) < _outputs.size());

    // _operatorTimer might already be paused e.g. when sendDataMsg() is called by sendControlMsg().
    bool timerOriginallyRunning = _operatorTimer.isRunning();
    if (timerOriginallyRunning) {
        _operatorTimer.pause();
    }
    ScopeGuard guard([&] {
        if (timerOriginallyRunning) {
            _operatorTimer.unpause();
        }
    });

    LOGV2_DEBUG(
        8241200, 1, "sendDataMsg", "operatorName"_attr = getName(), "dataMsg"_attr = dataMsg);
    if (controlMsg) {
        LOGV2_DEBUG(8241201,
                    1,
                    "sendDataMsg",
                    "operatorName"_attr = getName(),
                    "controlMsg"_attr = *controlMsg);
    }

    if (dataMsg.docs.empty()) {
        if (controlMsg) {
            sendControlMsg(outputIdx, std::move(*controlMsg));
        }

        // We don't send empty data messages.
        return;
    }

    OperatorStats stats;
    stats.numOutputDocs += dataMsg.docs.size();
    incOperatorStats(std::move(stats));

    auto& output = _outputs[outputIdx];
    output.oper->onDataMsg(output.operInputIdx, std::move(dataMsg), std::move(controlMsg));
}

void Operator::sendControlMsg(int32_t outputIdx, StreamControlMsg controlMsg) {
    LOGV2_DEBUG(8241202,
                1,
                "sendControlMsg",
                "operatorName"_attr = getName(),
                "controlMsg"_attr = controlMsg);

    // _operatorTimer might already be paused e.g. when sendControlMsg() is called by sendDataMsg().
    bool timerOriginallyRunning = _operatorTimer.isRunning();
    if (timerOriginallyRunning) {
        _operatorTimer.pause();
    }
    ScopeGuard guard([&] {
        if (timerOriginallyRunning) {
            _operatorTimer.unpause();
        }
    });

    dassert(size_t(outputIdx) < _outputs.size());
    if (controlMsg.checkpointMsg) {
        // This won't work as easily when we support multiple outputs for an Operator.
        invariant(outputIdx == 0 && _outputs.size() == 1);
        tassert(825102, "Expected checkpointStorage to be set.", _context->checkpointStorage);
        _context->checkpointStorage->addStats(controlMsg.checkpointMsg->id, _operatorId, _stats);
    }
    auto& output = _outputs[outputIdx];
    output.oper->onControlMsg(output.operInputIdx, std::move(controlMsg));
}

void Operator::setOperatorId(OperatorId operatorId) {
    _operatorId = operatorId;
}

}  // namespace streams
