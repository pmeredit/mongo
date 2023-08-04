/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "vector_search/document_source_vector_search.h"
#include "mongo/base/string_data.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "search/document_source_internal_search_id_lookup.h"
#include "search/lite_parsed_search.h"
#include "search/search_task_executors.h"
#include "vector_search/filter_validator.h"
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
      _filterExpr(_request.getFilter() ? uassertStatusOK(MatchExpressionParser::parse(
                                             *_request.getFilter(), pExpCtx))
                                       : nullptr),
      _taskExecutor(taskExecutor) {
    if (_filterExpr) {
        validateVectorSearchFilter(_filterExpr.get());
    }
}

Value DocumentSourceVectorSearch::serialize(const SerializationOptions& opts) const {
    // First, serialize the IDL struct.
    auto baseObj = [&] {
        BSONObjBuilder builder;
        _request.serialize(&builder, opts);
        return builder.obj();
    }();

    // IDL doesn't know how to shapify 'numCandidates' and 'limit', serialize them explicitly.
    baseObj =
        baseObj.addFields(BSON(VectorSearchSpec::kNumCandidatesFieldName
                               << opts.serializeLiteral(_request.getNumCandidates().coerceToLong())
                               << VectorSearchSpec::kLimitFieldName
                               << opts.serializeLiteral(_request.getLimit().coerceToLong())));

    if (_filterExpr) {
        // We need to serialize the parsed match expression rather than the generic BSON object to
        // correctly handle keywords.
        baseObj = baseObj.addFields(
            BSON(VectorSearchSpec::kFilterFieldName << _filterExpr->serialize(opts)));
    }

    // We don't want mongos to make a remote call to mongot even though it can generate explain
    // output.
    if (!opts.verbosity || pExpCtx->inMongos) {
        return Value(Document{{kStageName, baseObj}});
    }

    BSONObj explainInfo = _explainResponse.isEmpty()
        ? mongot_cursor::getKnnExplainResponse(pExpCtx, _request, _taskExecutor.get())
        : _explainResponse;

    baseObj = baseObj.addFields(BSON("explain" << opts.serializeLiteral(explainInfo)));
    return Value(Document{{kStageName, baseObj}});
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

    // Populate $sortKey metadata field so that mongos can properly merge sort the document stream.
    if (pExpCtx->needsMerge) {
        // Metadata can't be changed on a Document. Create a MutableDocument to set the sortKey.
        MutableDocument output(Document::fromBsonWithMetaData(response.value()));

        tassert(7828500,
                "Expected vector search distance to be present",
                output.metadata().hasVectorSearchScore());
        output.metadata().setSortKey(Value{output.metadata().getVectorSearchScore()},
                                     true /* isSingleElementKey */);
        return output.freeze();
    }
    return Document::fromBsonWithMetaData(response.value());
}

DocumentSource::GetNextResult DocumentSourceVectorSearch::doGetNext() {
    // Return EOF if pExpCtx->uuid is unset here; the collection we are searching over has not been
    // created yet.
    if (!pExpCtx->uuid) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    if (pExpCtx->explain) {
        _explainResponse =
            mongot_cursor::getKnnExplainResponse(pExpCtx, _request, _taskExecutor.get());
        return DocumentSource::GetNextResult::makeEOF();
    }

    // If this is the first call, establish the cursor.
    if (!_cursor) {
        _cursor.emplace(mongot_cursor::establishKnnCursor(pExpCtx, _request, _taskExecutor));
    }

    return getNextAfterSetup();
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceVectorSearch::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    const auto limit = elem.embeddedObject()
                           .getField(VectorSearchSpec::kLimitFieldName)
                           .parseIntegerElementToNonNegativeLong();
    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);
    uassert(7912700,
            str::stream() << "invalid argument to $limit stage: " << limit.getStatus().reason(),
            limit.isOK());
    auto serviceContext = expCtx->opCtx->getServiceContext();
    std::list<intrusive_ptr<DocumentSource>> desugaredPipeline = {
        make_intrusive<DocumentSourceVectorSearch>(
            VectorSearchSpec::parse(IDLParserContext(kStageName), elem.embeddedObject()),
            expCtx,
            executor::getMongotTaskExecutor(serviceContext))};

    // Only add an idLookup stage once, when we reach the mongod that will execute the pipeline.
    // Ignore the case where we have a stub 'mongoProcessInterface' because this only occurs during
    // validation/analysis, e.g. for QE and pipeline-style updates.
    if ((typeid(*expCtx->mongoProcessInterface) != typeid(StubMongoProcessInterface) &&
         !expCtx->mongoProcessInterface->inShardedEnvironment(expCtx->opCtx)) ||
        OperationShardingState::isComingFromRouter(expCtx->opCtx)) {
        desugaredPipeline.insert(std::next(desugaredPipeline.begin()),
                                 make_intrusive<DocumentSourceInternalSearchIdLookUp>(expCtx));
    }
    return desugaredPipeline;
}

}  // namespace mongo
