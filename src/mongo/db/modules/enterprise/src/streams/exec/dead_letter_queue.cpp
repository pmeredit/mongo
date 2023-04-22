/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/dead_letter_queue.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

DeadLetterQueue::DeadLetterQueue(NamespaceString ns) : _ns(std::move(ns)) {}

void DeadLetterQueue::addMessage(mongo::BSONObjBuilder objBuilder) {
    // Add metadata like "namespace" to the message.
    objBuilder.append("namespace", _ns.ns());
    return doAddMessage(objBuilder.obj());
}

}  // namespace streams
