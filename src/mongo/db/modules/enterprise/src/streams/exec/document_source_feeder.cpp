/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/document_source_feeder.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

ALLOCATE_DOCUMENT_SOURCE_ID(_feeder, DocumentSourceFeeder::id)

constexpr mongo::StringData kStageName = "$_feeder"_sd;

DocumentSourceFeeder::DocumentSourceFeeder(const boost::intrusive_ptr<ExpressionContext>& expCtx)
    : DocumentSource(kStageName, expCtx) {}

void DocumentSourceFeeder::addDocument(Document doc) {
    _docs.push(std::move(doc));
}

const char* DocumentSourceFeeder::getSourceName() const {
    return kStageName.rawData();
}

DocumentSource::GetNextResult DocumentSourceFeeder::doGetNext() {
    if (_docs.empty()) {
        return _endOfBufferSignal;
    }

    GetNextResult result(std::move(_docs.front()));
    _docs.pop();
    return result;
}

}  // namespace streams
