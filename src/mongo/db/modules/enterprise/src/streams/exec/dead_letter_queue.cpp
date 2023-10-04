/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/dead_letter_queue.h"

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/namespace_string_util.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

DeadLetterQueue::DeadLetterQueue(Context* context) : _context(context) {
    MetricManager::LabelsVec labels;
    labels.push_back(std::make_pair(kTenantIdLabelKey, _context->tenantId));
    labels.push_back(std::make_pair(kProcessorIdLabelKey, _context->streamProcessorId));
    _numDlqDocumentsCounter = _context->metricManager->registerCounter(
        "num_dlq_documents", "Number of documents inserted into the DLQ", std::move(labels));
}

void DeadLetterQueue::addMessage(mongo::BSONObjBuilder objBuilder) {
    _numDlqDocumentsCounter->increment();
    return doAddMessage(objBuilder.obj());
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

boost::optional<std::string> DeadLetterQueue::getError() {
    return doGetError();
}

}  // namespace streams
