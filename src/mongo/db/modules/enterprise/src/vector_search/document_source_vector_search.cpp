/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "vector_search/document_source_vector_search.h"

#include "search/lite_parsed_search.h"
#include "search/search_task_executors.h"
#include "vector_search/mongot_cursor.h"

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
    VectorSearchSpec&& request,
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    std::shared_ptr<executor::TaskExecutor> taskExecutor)
    : DocumentSource(kStageName, expCtx),
      _request(std::move(request)),
      _taskExecutor(taskExecutor),
      _limit(_request.getLimit().coerceToLong()) {}

Value DocumentSourceVectorSearch::serialize(SerializationOptions opts) const {
    // First, serialize the IDL struct.
    auto baseObj = [&] {
        BSONObjBuilder builder;
        _request.serialize(&builder, opts);
        return builder.obj();
    }();

    // IDL doesn't know how to shapify 'candidates' and 'limit', serialize them explicitly.
    baseObj = baseObj.addFields(
        BSON(VectorSearchSpec::kCandidatesFieldName
             << opts.serializeLiteral(_request.getCandidates().coerceToLong())
             << VectorSearchSpec::kLimitFieldName << opts.serializeLiteral(_limit)));

    if (_request.getFilter()) {
        // TODO SERVER-78283 Parse the match expression once for validation and then save it to
        // reuse here.
        auto expr = uassertStatusOK(MatchExpressionParser::parse(*_request.getFilter(), pExpCtx));
        // We need to serialize the parsed match expression rather than the generic BSON object to
        // correctly handle keywords.
        baseObj =
            baseObj.addFields(BSON(VectorSearchSpec::kFilterFieldName << expr->serialize(opts)));
    }

    return Value(Document{{kStageName, baseObj}});
}

bool DocumentSourceVectorSearch::shouldReturnEOF() {
    if (_docsReturned >= _limit) {
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
        VectorSearchSpec::parse(IDLParserContext(kStageName), elem.embeddedObject()),
        expCtx,
        executor::getMongotTaskExecutor(serviceContext));
}

}  // namespace mongo
