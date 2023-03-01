/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "streams/exec/operator.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

namespace streams {

using namespace mongo;

Operator::Operator(int32_t numInputs, int32_t numOutputs)
    : _numInputs(numInputs), _numOutputs(numOutputs) {
    _outputs.reserve(_numOutputs);
}

void Operator::addOutput(Operator* oper, int32_t operInputIdx) {
    dassert(_outputs.size() < size_t(_numOutputs));
    _outputs.push_back(OutputInfo{oper, operInputIdx});
}

void Operator::start() {
    dassert(_outputs.size() == size_t(_numOutputs));
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
    auto& output = _outputs[outputIdx];
    output.oper->onDataMsg(output.operInputIdx, std::move(dataMsg), std::move(controlMsg));
}

void Operator::sendControlMsg(int32_t outputIdx, StreamControlMsg controlMsg) {
    dassert(size_t(outputIdx) < _outputs.size());
    auto& output = _outputs[outputIdx];
    output.oper->onControlMsg(output.operInputIdx, std::move(controlMsg));
}

}  // namespace streams
