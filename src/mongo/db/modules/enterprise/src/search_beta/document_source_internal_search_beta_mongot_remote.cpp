/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_internal_search_beta_mongot_remote.h"

#include "mongo/db/curop.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/db/service_context.h"
#include "mongo/executor/non_auth_task_executor.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/transport/transport_layer.h"
#include "mongot_options.h"

namespace mongo {

using boost::intrusive_ptr;
using executor::RemoteCommandRequest;
using executor::TaskExecutorCursor;

REGISTER_DOCUMENT_SOURCE(_internalSearchBetaMongotRemote,
                         DocumentSourceInternalSearchBetaMongotRemote::LiteParsed::parse,
                         DocumentSourceInternalSearchBetaMongotRemote::createFromBson);

MONGO_FAIL_POINT_DEFINE(searchBetaReturnEofImmediately);

const char* DocumentSourceInternalSearchBetaMongotRemote::getSourceName() const {
    return "$_internalSearchBetaMongotRemote";
}

Value DocumentSourceInternalSearchBetaMongotRemote::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << Document(_searchBetaQuery)));
}

void DocumentSourceInternalSearchBetaMongotRemote::populateCursor() {
    invariant(!_cursor);

    uassert(31082,
            str::stream() << "$searchBeta not enabled! "
                          << "Enable SearchBeta by setting serverParameter mongotHost to a valid "
                          << "\"host:port\" string",
            globalMongotParams.enabled);
    auto swHostAndPort = HostAndPort::parse(globalMongotParams.host);

    // This host and port string is configured and validated at startup.
    invariant(swHostAndPort.getStatus().isOK());

    RemoteCommandRequest rcr(RemoteCommandRequest(swHostAndPort.getValue(),
                                                  pExpCtx->ns.db().toString(),
                                                  commandObject(_searchBetaQuery, pExpCtx),
                                                  pExpCtx->opCtx));
    rcr.sslMode = transport::ConnectSSLMode::kDisableSSL;

    _cursor.emplace(executor::getNonAuthTaskExecutor(pExpCtx->opCtx->getServiceContext()), rcr);
}

/**
 * Gets the next result from mongot using a TaskExecutorCursor.
 */
DocumentSource::GetNextResult DocumentSourceInternalSearchBetaMongotRemote::getNext() {
    pExpCtx->checkForInterrupt();

    if (MONGO_FAIL_POINT(searchBetaReturnEofImmediately)) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    if (!_cursor) {
        populateCursor();
    }

    auto response = _cursor->getNext(pExpCtx->opCtx);
    auto& opDebug = CurOp::get(pExpCtx->opCtx)->debug();

    if (opDebug.msWaitingForMongot) {
        *opDebug.msWaitingForMongot += durationCount<Milliseconds>(_cursor->resetWaitingTime());
    } else {
        opDebug.msWaitingForMongot = durationCount<Milliseconds>(_cursor->resetWaitingTime());
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

intrusive_ptr<DocumentSource> DocumentSourceInternalSearchBetaMongotRemote::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(31067, "SearchBeta argument must be an object.", elem.type() == BSONType::Object);
    return new DocumentSourceInternalSearchBetaMongotRemote(elem.embeddedObject(), pExpCtx);
}

BSONObj DocumentSourceInternalSearchBetaMongotRemote::commandObject(
    const BSONObj& query, const intrusive_ptr<ExpressionContext>& expCtx) {
    return BSON("searchBeta" << expCtx->ns.coll() << "collectionUUID" << expCtx->uuid.get()
                             << "query"
                             << query);
}

}  // namespace mongo
