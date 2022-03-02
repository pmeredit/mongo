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
        return Value(DOC(getSourceName() << Document(_searchQuery)));
    }
    // Explain with queryPlanner verbosity does not execute the query, so the _explainResponse
    // may not be populated. In that case, we fetch the response here instead.
    BSONObj explainInfo = _explainResponse.isEmpty()
        ? mongot_cursor::getExplainResponse(pExpCtx, _searchQuery, _taskExecutor)
        : _explainResponse;
    return Value(DOC(getSourceName() << DOC("mongotQuery" << Document(_searchQuery) << "explain"
                                                          << Document(explainInfo))));
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
                    !::mongo::feature_flags::gFeatureFlagSearchMeta.isEnabledAndIgnoreFCV());
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

    return Document::fromBsonWithMetaData(response.get());
}

intrusive_ptr<DocumentSource> DocumentSourceInternalSearchMongotRemote::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(31067, "Search argument must be an object.", elem.type() == BSONType::Object);
    auto serviceContext = expCtx->opCtx->getServiceContext();
    return new DocumentSourceInternalSearchMongotRemote(
        elem.embeddedObject(), expCtx, executor::getMongotTaskExecutor(serviceContext));
}

bool DocumentSourceInternalSearchMongotRemote::skipSearchStageRemoteSetup() {
    return MONGO_unlikely(searchReturnEofImmediately.shouldFail());
}

}  // namespace mongo
