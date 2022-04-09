/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_internal_search_mongot_remote.h"

#include "mongo/db/curop.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/service_context.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_cursor.h"
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

REGISTER_DOCUMENT_SOURCE(_internalSearchMongotRemote,
                         DocumentSourceInternalSearchMongotRemote::LiteParsed::parse,
                         DocumentSourceInternalSearchMongotRemote::createFromBson,
                         AllowedWithApiStrict::kAlways);

MONGO_FAIL_POINT_DEFINE(searchReturnEofImmediately);

const char* DocumentSourceInternalSearchMongotRemote::getSourceName() const {
    return kStageName.rawData();
}

Value DocumentSourceInternalSearchMongotRemote::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    // Though mongos can generate explain output, it should never make a remote call to the mongot.
    if (!explain || pExpCtx->inMongos) {
        if (_metadataMergeProtocolVersion) {
            MutableDocument mDoc;
            mDoc.addField(kSearchQueryField, Value(_searchQuery));
            mDoc.addField(kProtocolVersionField, Value(_metadataMergeProtocolVersion.get()));
            mDoc.addField(kMergingPipelineField, Value(_mergingPipeline->serialize()));
            return Value(Document{{getSourceName(), mDoc.freezeToValue()}});
        } else {
            return Value(Document{{getSourceName(), _searchQuery}});
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
    return Value(DOC(getSourceName() << mDoc.freeze()));
}

boost::optional<BSONObj> DocumentSourceInternalSearchMongotRemote::_getNext() {
    try {
        return _cursor->getNext(pExpCtx->opCtx);
    } catch (DBException& ex) {
        ex.addContext("Remote error from mongot");
        throw;
    }
}

DocumentSource::GetNextResult DocumentSourceInternalSearchMongotRemote::doGetNext() {
    if (MONGO_unlikely(searchReturnEofImmediately.shouldFail())) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    // Return EOF if pExpCtx->uuid is unset here; the collection we are searching over has not been
    // created yet.
    if (!pExpCtx->uuid) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    if (pExpCtx->explain) {
        _explainResponse = mongot_cursor::getExplainResponse(pExpCtx, _searchQuery, _taskExecutor);
        return DocumentSource::GetNextResult::makeEOF();
    }

    // If the collection is sharded we should have a cursor already. Otherwise establish it now.
    if (!_cursor && !_dispatchedQuery) {
        tassert(6253505,
                "Expected to have a cursor already in a sharded pipeline",
                !pExpCtx->needsMerge ||
                    !::mongo::feature_flags::gFeatureFlagSearchShardedFacets.isEnabled(
                        serverGlobalParams.featureCompatibility));
        auto cursors = mongot_cursor::establishCursors(pExpCtx, _searchQuery, _taskExecutor);
        _dispatchedQuery = true;
        tassert(5253301, "Expected exactly one cursor from mongot", cursors.size() == 1);
        _cursor.emplace(std::move(cursors[0]));
    }

    auto response = _getNext();
    auto& opDebug = CurOp::get(pExpCtx->opCtx)->debug();

    if (opDebug.msWaitingForMongot) {
        *opDebug.msWaitingForMongot += durationCount<Milliseconds>(_cursor->resetWaitingTime());
    } else {
        opDebug.msWaitingForMongot = durationCount<Milliseconds>(_cursor->resetWaitingTime());
    }
    opDebug.mongotBatchNum = _cursor->getBatchNum();

    // Meta variables will be constant across the query and only need to be set once.
    // This feature was backported and is available on versions after 4.4. Therefore there is no
    // need to check FCV, as downgrading should not cause any issues.
    if (!pExpCtx->variables.hasConstantValue(Variables::kSearchMetaId) && _cursor &&
        _cursor->getCursorVars() &&
        ::mongo::feature_flags::gFeatureFlagSearchMeta.isEnabledAndIgnoreFCV()) {
        // Variables on the cursor must be an object.
        auto varsObj = Value(_cursor->getCursorVars().get());
        auto metaVal = varsObj.getDocument().getField(
            Variables::getBuiltinVariableName(Variables::kSearchMetaId));
        if (!metaVal.missing()) {
            pExpCtx->variables.setReservedValue(Variables::kSearchMetaId, metaVal, true);
        }
    }
    // The TaskExecutorCursor will store '0' as its CursorId if the cursor to mongot is exhausted.
    // If we already have a cursorId from a previous call, just use that.
    if (!_cursorId) {
        _cursorId = _cursor->getCursorId();
    }
    opDebug.mongotCursorId = _cursorId;

    if (!response) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    // For now, sort is always on '$searchScore'. Metadata is only present if the data needs to be
    // merged.
    if (pExpCtx->needsMerge) {
        // Metadata can't be changed on a Document. Create a MutableDocument to set the sortKey.
        MutableDocument output(Document::fromBsonWithMetaData(response.get()));
        // If this stage is getting metadata documents from mongot, those don't include searchScore.
        if (output.metadata().hasSearchScore()) {
            output.metadata().setSortKey(Value{output.metadata().getSearchScore()},
                                         true /* isSingleElementKey */);
        }
        return output.freeze();
    }
    return Document::fromBsonWithMetaData(response.get());
}

intrusive_ptr<DocumentSource> DocumentSourceInternalSearchMongotRemote::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(31067, "Search argument must be an object.", elem.type() == BSONType::Object);
    auto serviceContext = expCtx->opCtx->getServiceContext();
    auto obj = elem.embeddedObject();
    // If creating from a user request, BSON is just an object with a search query. Otherwise it has
    // both these fields.
    if (obj.hasField(kProtocolVersionField) || obj.hasField(kSearchQueryField)) {
        uassert(6253711,
                "'metadataMergeProtocolVersion' and 'searchQuery' are only allowed together",
                obj.hasField(kSearchQueryField) && obj.hasField(kProtocolVersionField));
        auto query = obj.getField(kSearchQueryField);
        auto version = obj.getField(kProtocolVersionField);
        uassert(6253712, "'searchQuery' must be an object", query.type() == BSONType::Object);
        uassert(6253713,
                "'metadataMergeProtocolVersion' must be an int",
                version.type() == BSONType::NumberInt);
        std::unique_ptr<Pipeline, PipelineDeleter> searchMetaMergePipe;
        if (auto mergePipe = obj[kMergingPipelineField]) {
            searchMetaMergePipe = Pipeline::parseFromArray(mergePipe, expCtx);
        }
        return new DocumentSourceInternalSearchMongotRemote(
            query.embeddedObject(),
            expCtx,
            executor::getMongotTaskExecutor(serviceContext),
            version.Int(),
            std::move(searchMetaMergePipe));
    }
    return new DocumentSourceInternalSearchMongotRemote(
        obj, expCtx, executor::getMongotTaskExecutor(serviceContext));
}

DocumentSourceInternalSearchMongotRemote::DocumentSourceInternalSearchMongotRemote(
    const BSONObj& query,
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    executor::TaskExecutor* taskExecutor,
    int metadataMergeProtocolVersion,
    std::unique_ptr<Pipeline, PipelineDeleter> mergePipeline)
    : DocumentSource(kStageName, expCtx),
      _searchQuery(query.getOwned()),
      _taskExecutor(taskExecutor),
      _metadataMergeProtocolVersion(metadataMergeProtocolVersion),
      _mergingPipeline(std::move(mergePipeline)) {}

bool DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup() {
    return MONGO_unlikely(searchReturnEofImmediately.shouldFail());
}

}  // namespace mongo
