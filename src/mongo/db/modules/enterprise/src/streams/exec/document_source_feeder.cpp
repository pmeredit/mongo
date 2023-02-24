/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "streams/exec/document_source_feeder.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

namespace streams {

using namespace mongo;

constexpr mongo::StringData kStageName = "$_feeder"_sd;

DocumentSourceFeeder::DocumentSourceFeeder(const boost::intrusive_ptr<ExpressionContext>& expCtx)
    : DocumentSource(kStageName, expCtx),
      _pauseSignal(DocumentSource::GetNextResult::makePauseExecution()) {}

void DocumentSourceFeeder::addDocument(Document doc) {
    _docs.push(std::move(doc));
}

const char* DocumentSourceFeeder::getSourceName() const {
    return kStageName.rawData();
}

DocumentSource::GetNextResult DocumentSourceFeeder::doGetNext() {
    if (_docs.empty()) {
        return _pauseSignal;
    }

    GetNextResult result(std::move(_docs.front()));
    _docs.pop();
    return result;
}

}  // namespace streams
