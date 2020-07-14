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
#include "mongo/transport/transport_layer.h"
#include "mongot_options.h"
#include "mongot_task_executor.h"

namespace mongo {

using boost::intrusive_ptr;
using executor::RemoteCommandRequest;
using executor::TaskExecutorCursor;

REGISTER_DOCUMENT_SOURCE(_internalSearchMongotRemote,
                         DocumentSourceInternalSearchMongotRemote::LiteParsed::parse,
                         DocumentSourceInternalSearchMongotRemote::createFromBson);

MONGO_FAIL_POINT_DEFINE(searchReturnEofImmediately);

const char* DocumentSourceInternalSearchMongotRemote::getSourceName() const {
    return kStageName.rawData();
}

Value DocumentSourceInternalSearchMongotRemote::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << Document(_searchQuery)));
}

void DocumentSourceInternalSearchMongotRemote::populateCursor() {
    invariant(!_cursor);

    uassert(31082,
            str::stream() << "$search not enabled! "
                          << "Enable Search by setting serverParameter mongotHost to a valid "
                          << "\"host:port\" string",
            globalMongotParams.enabled);
    auto swHostAndPort = HostAndPort::parse(globalMongotParams.host);

    // This host and port string is configured and validated at startup.
    invariant(swHostAndPort.getStatus().isOK());

    RemoteCommandRequest rcr(RemoteCommandRequest(swHostAndPort.getValue(),
                                                  pExpCtx->ns.db().toString(),
                                                  commandObject(_searchQuery, pExpCtx),
                                                  pExpCtx->opCtx));
    rcr.sslMode = transport::ConnectSSLMode::kDisableSSL;

    _cursor.emplace(_taskExecutor, rcr);
}

/**
 * Gets the next result from mongot using a TaskExecutorCursor and add an error context if any.
 */
boost::optional<BSONObj> DocumentSourceInternalSearchMongotRemote::_getNext() {
    try {
        return _cursor->getNext(pExpCtx->opCtx);
    } catch (DBException& ex) {
        ex.addContext("Remote error from mongot");
        throw;
    }
}

/**
 * Gets the next result from mongot using a TaskExecutorCursor.
 */
DocumentSource::GetNextResult DocumentSourceInternalSearchMongotRemote::doGetNext() {
    if (MONGO_unlikely(searchReturnEofImmediately.shouldFail())) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    // Return EOF if pExpCtx->uuid is unset here; the collection we are searching over has not been
    // created yet.
    if (!pExpCtx->uuid) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    if (!_cursor) {
        populateCursor();
    }

    auto response = _getNext();
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

intrusive_ptr<DocumentSource> DocumentSourceInternalSearchMongotRemote::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(31067, "Search argument must be an object.", elem.type() == BSONType::Object);
    auto serviceContext = expCtx->opCtx->getServiceContext();
    return new DocumentSourceInternalSearchMongotRemote(
        elem.embeddedObject(), expCtx, executor::getMongotTaskExecutor(serviceContext));
}

BSONObj DocumentSourceInternalSearchMongotRemote::commandObject(
    const BSONObj& query, const intrusive_ptr<ExpressionContext>& expCtx) {
    return BSON("search" << expCtx->ns.coll() << "collectionUUID" << expCtx->uuid.get() << "query"
                         << query);
}

}  // namespace mongo
