/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/dead_letter_queue.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/namespace_string_util.h"
#include "mongo/util/time_support.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/message.h"
#include "streams/exec/output_sampler.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

DeadLetterQueue::DeadLetterQueue(Context* context) : _context(context) {}

int DeadLetterQueue::addMessage(mongo::BSONObjBuilder objBuilder) {
    objBuilder.append("processorName", _context->streamName);
    if (_context->instanceName) {
        objBuilder.append("instanceName", *_context->instanceName);
    }
    objBuilder.append("dlqTime", Date_t::now());
    auto obj = objBuilder.obj();
    LOGV2_DEBUG(8241203, 1, "dlqMessage", "msg"_attr = obj, "context"_attr = _context);
    sendOutputToSamplers(obj);
    auto dlqBytes = doAddMessage(std::move(obj));
    _numDlqDocumentsCounter->increment();
    _numDlqBytesCounter->increment(dlqBytes);
    return dlqBytes;
}

void DeadLetterQueue::registerMetrics(MetricManager* metricManager) {
    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, _context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, _context->streamProcessorId));
    labels.push_back(std::make_pair(kProcessorNameLabelKey, _context->streamName));
    _numDlqDocumentsCounter = metricManager->registerCounter(
        "num_dlq_documents", "Number of documents inserted into the DLQ", labels);
    _numDlqBytesCounter = metricManager->registerCounter(
        "num_dlq_bytes", "Number of bytes inserted into the DLQ", labels);
}

void DeadLetterQueue::start() {
    doStart();
}

void DeadLetterQueue::stop() {
    doStop();
}

void DeadLetterQueue::flush() {
    doFlush();
}

SPStatus DeadLetterQueue::getStatus() {
    return doGetStatus();
}

void DeadLetterQueue::addOutputSampler(boost::intrusive_ptr<OutputSampler> sampler) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    _outputSamplers.emplace_back(std::move(sampler));
}

void DeadLetterQueue::sendOutputToSamplers(const BSONObj& msg) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    if (_outputSamplers.empty()) {
        return;
    }

    StreamDataMsg dataMsg;
    BSONObjBuilder builder;
    builder.append("_dlqMessage", msg);
    dataMsg.docs.push_back(Document{builder.obj()});
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
