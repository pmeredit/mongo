/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/kafka_partition_consumer_base.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

KafkaPartitionConsumerBase::KafkaPartitionConsumerBase(Context* context, Options options)
    : _context(context), _options(std::move(options)) {
    _sourceBufferHandle = _context->sourceBufferManager->registerSourceBuffer();
}

KafkaPartitionConsumerBase::~KafkaPartitionConsumerBase() {
    // Report 0 memory usage to SourceBufferManager.
    _context->sourceBufferManager->allocPages(
        _sourceBufferHandle.get(), 0 /* curSize */, 0 /* numPages */);
}

}  // namespace streams
