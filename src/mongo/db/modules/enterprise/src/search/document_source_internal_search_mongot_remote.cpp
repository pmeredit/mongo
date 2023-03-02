/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "document_source_internal_search_mongot_remote.h"

#include "document_source_internal_search_id_lookup.h"
#include "lite_parsed_search.h"
#include "mongo/db/curop.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_single_document_transformation.h"
#include "mongo/db/service_context.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/logv2/log.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/transport/transport_layer.h"
#include "mongot_cursor.h"
#include "mongot_options.h"
#include "mongot_task_executor.h"

namespace mongo {
namespace {
const std::string kSearchQueryField = "mongotQuery";
const std::string kProtocolVersionField = "metadataMergeProtocolVersion";
const std::string kMergingPipelineField = "mergingPipeline";
}  // namespace

using boost::intrusive_ptr;
using executor::RemoteCommandRequest;
using executor::TaskExecutorCursor;

REGISTER_DOCUMENT_SOURCE_CONDITIONALLY(_internalSearchMongotRemote,
                                       LiteParsedSearchStage::parse,
                                       DocumentSourceInternalSearchMongotRemote::createFromBson,
                                       AllowedWithApiStrict::kInternal,
                                       AllowedWithClientType::kInternal,
                                       boost::none,
                                       true);

MONGO_FAIL_POINT_DEFINE(searchReturnEofImmediately);

const char* DocumentSourceInternalSearchMongotRemote::getSourceName() const {
    return kStageName.rawData();
}

Document DocumentSourceInternalSearchMongotRemote::serializeWithoutMergePipeline(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    // Though mongos can generate explain output, it should never make a remote call to the mongot.
    if (!explain || pExpCtx->inMongos) {
        if (_metadataMergeProtocolVersion) {
            MutableDocument spec;
            spec.addField(kSearchQueryField, Value(_searchQuery));
            spec.addField(kProtocolVersionField, Value(_metadataMergeProtocolVersion.get()));
            // In a non-sharded scenario we don't need to pass the limit around as the limit stage
            // will do equivalent work. In a sharded scenario we want the limit to get to the
            // shards, so we serialize it. We serialize it in this block as all sharded search
            // queries have a protocol version.
            // This is the limit that we copied, and does not replace the real limit stage later in
            // the pipeline.
            spec.addField(InternalSearchMongotRemoteSpec::kLimitFieldName,
                          Value((long long)_limit));
            if (_sortSpec.has_value()) {
                spec.addField(InternalSearchMongotRemoteSpec::kSortSpecFieldName,
                              Value{*_sortSpec});
            }
            return spec.freeze();
        } else {
            return Document{_searchQuery};
        }
    }
    // Explain with queryPlanner verbosity does not execute the query, so the _explainResponse
    // may not be populated. In that case, we fetch the response here instead.
    BSONObj explainInfo = _explainResponse.isEmpty()
        ? mongot_cursor::getExplainResponse(pExpCtx, _searchQuery, _taskExecutor)
        : _explainResponse;
    MutableDocument mDoc;
    mDoc.addField(kSearchQueryField, Value(_searchQuery));
    mDoc.addField("explain", Value(explainInfo));
    // Limit is relevant for explain.
    if (_limit != 0) {
        mDoc.addField(InternalSearchMongotRemoteSpec::kLimitFieldName, Value((long long)_limit));
    }
    if (_sortSpec.has_value()) {
        mDoc.addField(InternalSearchMongotRemoteSpec::kSortSpecFieldName, Value{*_sortSpec});
    }
    return mDoc.freeze();
}

Value DocumentSourceInternalSearchMongotRemote::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    MutableDocument innerSpec{serializeWithoutMergePipeline(explain)};
    if ((!explain || pExpCtx->inMongos) && _metadataMergeProtocolVersion) {
        innerSpec[kMergingPipelineField] = Value(_mergingPipeline->serialize(explain));
    }
    return Value(Document{{getSourceName(), innerSpec.freezeToValue()}});
}

boost::optional<BSONObj> DocumentSourceInternalSearchMongotRemote::_getNext() {
    try {
        return _cursor->getNext(pExpCtx->opCtx);
    } catch (DBException& ex) {
        ex.addContext("Remote error from mongot");
        throw;
    }
}

bool DocumentSourceInternalSearchMongotRemote::shouldReturnEOF() {
    if (MONGO_unlikely(searchReturnEofImmediately.shouldFail())) {
        return true;
    }

    if (_limit != 0 && _docsReturned >= _limit) {
        return true;
    }

    // Return EOF if pExpCtx->uuid is unset here; the collection we are searching over has not been
    // created yet.
    if (!pExpCtx->uuid) {
        return true;
    }

    if (pExpCtx->explain) {
        _explainResponse = mongot_cursor::getExplainResponse(pExpCtx, _searchQuery, _taskExecutor);
        return true;
    }
    return false;
}

void DocumentSourceInternalSearchMongotRemote::tryToSetSearchMetaVar() {
    // Meta variables will be constant across the query and only need to be set once.
    // This feature was backported and is available on versions after 4.4. Therefore there is no
    // need to check FCV, as downgrading should not cause any issues.
    if (!pExpCtx->variables.hasConstantValue(Variables::kSearchMetaId) && _cursor &&
        _cursor->getCursorVars()) {
        // Variables on the cursor must be an object.
        auto varsObj = Value(_cursor->getCursorVars().value());
        auto metaVal = varsObj.getDocument().getField(
            Variables::getBuiltinVariableName(Variables::kSearchMetaId));
        if (!metaVal.missing()) {
            pExpCtx->variables.setReservedValue(Variables::kSearchMetaId, metaVal, true);
        }
    }
}

DocumentSource::GetNextResult DocumentSourceInternalSearchMongotRemote::getNextAfterSetup() {
    auto response = _getNext();
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
    // Populate $sortKey metadata field so that mongos can properly merge sort the document stream.
    if (pExpCtx->needsMerge) {
        // Metadata can't be changed on a Document. Create a MutableDocument to set the sortKey.
        MutableDocument output(Document::fromBsonWithMetaData(response.value()));

        // If we have a sortSpec, then we use that to set sortKey. Otherwise, use the 'searchScore'
        // if the document has one.
        if (_sortSpec.has_value()) {
            tassert(7320402,
                    "_sortKeyGen must be initialized if _sortSpec is present",
                    _sortKeyGen.has_value());
            auto sortKey = _sortKeyGen->computeSortKeyFromDocument(Document(*response));
            output.metadata().setSortKey(sortKey, _sortKeyGen->isSingleElementKey());
        } else if (output.metadata().hasSearchScore()) {
            // If this stage is getting metadata documents from mongot, those don't include
            // searchScore.
            output.metadata().setSortKey(Value{output.metadata().getSearchScore()},
                                         true /* isSingleElementKey */);
        }
        return output.freeze();
    }
    return Document::fromBsonWithMetaData(response.value());
}

executor::TaskExecutorCursor DocumentSourceInternalSearchMongotRemote::establishCursor() {
    auto cursors = mongot_cursor::establishCursors(pExpCtx, _searchQuery, _taskExecutor);
    // Should be called only in unsharded scenario, therefore only expect a results cursor and no
    // metadata cursor.
    tassert(5253301, "Expected exactly one cursor from mongot", cursors.size() == 1);
    return std::move(cursors[0]);
}

DocumentSource::GetNextResult DocumentSourceInternalSearchMongotRemote::doGetNext() {
    if (shouldReturnEOF()) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    // If the collection is sharded we should have a cursor already. Otherwise establish it now.
    if (!_cursor && !_dispatchedQuery) {
        _cursor.emplace(establishCursor());
        _dispatchedQuery = true;
    }
    tryToSetSearchMetaVar();

    return getNextAfterSetup();
}

intrusive_ptr<DocumentSource> DocumentSourceInternalSearchMongotRemote::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(31067, "Search argument must be an object.", elem.type() == BSONType::Object);
    auto serviceContext = expCtx->opCtx->getServiceContext();
    return new DocumentSourceInternalSearchMongotRemote(
        InternalSearchMongotRemoteSpec::parse(IDLParserContext(kStageName), elem.embeddedObject()),
        expCtx,
        executor::getMongotTaskExecutor(serviceContext));
}

Pipeline::SourceContainer::iterator DocumentSourceInternalSearchMongotRemote::doOptimizeAt(
    Pipeline::SourceContainer::iterator itr, Pipeline::SourceContainer* container) {
    // 'itr' points to this stage, don't need to look at that that.
    for (auto optItr = std::next(itr); optItr != container->end(); ++optItr) {
        auto limitStage = dynamic_cast<DocumentSourceLimit*>(optItr->get());
        // Copy the existing limit stage, but leave it in the pipeline in case this is a sharded
        // environment and it is needed on the merging node.
        if (limitStage) {
            _limit = limitStage->getLimit();
            break;
        }

        if (!optItr->get()->constraints().canSwapWithSkippingOrLimitingStage) {
            break;
        }
    }
    return std::next(itr);
}

bool DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup() {
    return MONGO_unlikely(searchReturnEofImmediately.shouldFail());
}

}  // namespace mongo
