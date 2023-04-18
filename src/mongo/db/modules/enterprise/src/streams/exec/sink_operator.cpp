/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <algorithm>

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "streams/exec/output_sampler.h"
#include "streams/exec/sink_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

void SinkOperator::addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler) {
    dassert(sampler);
    _outputSamplers.push_back(std::move(sampler));
}

void SinkOperator::sendOutputToSamplers(const StreamDataMsg& dataMsg) {
    if (_outputSamplers.empty()) {
        return;
    }

    for (auto& sampler : _outputSamplers) {
        sampler->addDataMsg(dataMsg);
    }

    // Prune the samplers that are done sampling.
    _outputSamplers.erase(std::remove_if(_outputSamplers.begin(),
                                         _outputSamplers.end(),
                                         [](const auto& sampler) {
                                             return sampler->doneSampling() ||
                                                 sampler->isCancelled();
                                         }),
                          _outputSamplers.end());
}

}  // namespace streams
