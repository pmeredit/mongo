/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/constants.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

Operator::Operator(Context* context, int32_t numInputs, int32_t numOutputs)
    : _context(context), _numInputs(numInputs), _numOutputs(numOutputs) {
    _outputs.reserve(_numOutputs);
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

    _stats.numInputDocs += dataMsg.docs.size();
    if (shouldComputeInputByteStats()) {
        for (const auto& doc : dataMsg.docs) {
            _stats.numInputBytes += doc.doc.getCurrentApproximateSize();
        }
    }
    doOnDataMsg(inputIdx, std::move(dataMsg), std::move(controlMsg));
}

void Operator::onControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    dassert(inputIdx < _numInputs);
    doOnControlMsg(inputIdx, std::move(controlMsg));
}

void Operator::sendDataMsg(int32_t outputIdx,
                           StreamDataMsg dataMsg,
                           boost::optional<StreamControlMsg> controlMsg) {
    dassert(size_t(outputIdx) < _outputs.size());

    _stats.numOutputDocs += dataMsg.docs.size();
    auto& output = _outputs[outputIdx];
    output.oper->onDataMsg(output.operInputIdx, std::move(dataMsg), std::move(controlMsg));
}

void Operator::sendControlMsg(int32_t outputIdx, StreamControlMsg controlMsg) {
    dassert(size_t(outputIdx) < _outputs.size());
    auto& output = _outputs[outputIdx];
    output.oper->onControlMsg(output.operInputIdx, std::move(controlMsg));
}

void Operator::incOperatorStats(OperatorStats stats) {
    _stats += stats;
}

}  // namespace streams
