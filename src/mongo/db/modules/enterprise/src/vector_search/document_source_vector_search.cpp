/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "vector_search/document_source_vector_search.h"

#include "search/lite_parsed_search.h"
#include "search/mongot_cursor.h"
#include "search/search_task_executors.h"

namespace mongo {

using boost::intrusive_ptr;
using executor::RemoteCommandRequest;
using executor::TaskExecutorCursor;

REGISTER_DOCUMENT_SOURCE_WITH_FEATURE_FLAG(vectorSearch,
                                           LiteParsedSearchStage::parse,
                                           DocumentSourceVectorSearch::createFromBson,
                                           AllowedWithApiStrict::kNeverInVersion1,
                                           feature_flags::gFeatureFlagVectorSearchPublicPreview);

DocumentSourceVectorSearch::DocumentSourceVectorSearch(
    const BSONObj& request,
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    std::shared_ptr<executor::TaskExecutor> taskExecutor)
    : DocumentSource(kStageName, expCtx),
      _request(request),
      _taskExecutor(taskExecutor),
      _candidates(request.getField(mongot_cursor::kCandidatesField).Int()) {}

Value DocumentSourceVectorSearch::serialize(SerializationOptions opts) const {
    // TODO SERVER-78279 Serialize the parameters individually.
    uasserted(ErrorCodes::NotImplemented, "DocumentSourceVectorSearch::serialize()");
    return Value(Document{{kStageName, opts.serializeLiteral(_request)}});
}

bool DocumentSourceVectorSearch::shouldReturnEOF() {
    if (_candidates != 0 && _docsReturned >= _candidates) {
        return true;
    }

    // Return EOF if pExpCtx->uuid is unset here; the collection we are searching over has not been
    // created yet.
    if (!pExpCtx->uuid) {
        return true;
    }

    return false;
}

boost::optional<BSONObj> DocumentSourceVectorSearch::getNext() {
    try {
        return _cursor->getNext(pExpCtx->opCtx);
    } catch (DBException& ex) {
        ex.addContext("Remote error from mongot");
        throw;
    }
}

DocumentSource::GetNextResult DocumentSourceVectorSearch::getNextAfterSetup() {
    auto response = getNext();
    auto& opDebug = CurOp::get(pExpCtx->opCtx)->debug();

    if (opDebug.msWaitingForMongot) {
        *opDebug.msWaitingForMongot += durationCount<Milliseconds>(_cursor->resetWaitingTime());
    } else {
        opDebug.msWaitingForMongot = durationCount<Milliseconds>(_cursor->resetWaitingTime());
    }
    opDebug.mongotBatchNum = _cursor->getBatchNum();

    // The TaskExecutorCursor will store '0' as its CursorId if the cursor to mongot is exhausted.
    // If we already have a cursorId from a previous call, just use that.
    if (!_cursorId) {
        _cursorId = _cursor->getCursorId();
    }

    opDebug.mongotCursorId = _cursorId;

    if (!response) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    ++_docsReturned;
    return Document::fromBsonWithMetaData(response.value());
}

DocumentSource::GetNextResult DocumentSourceVectorSearch::doGetNext() {
    if (shouldReturnEOF()) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    // If this is the first call, establish the cursor.
    if (!_cursor) {
        _cursor.emplace(mongot_cursor::establishKnnCursor(pExpCtx, _request, _taskExecutor));
    }

    return getNextAfterSetup();
}

intrusive_ptr<DocumentSource> DocumentSourceVectorSearch::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);
    auto serviceContext = expCtx->opCtx->getServiceContext();
    return new DocumentSourceVectorSearch(
        elem.embeddedObject(), expCtx, executor::getMongotTaskExecutor(serviceContext));
}

}  // namespace mongo
