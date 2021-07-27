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
    BSONObj explainInfo = _explainResponse.isEmpty() ? getExplainResponse() : _explainResponse;
    return Value(DOC(getSourceName() << DOC("mongotQuery" << Document(_searchQuery) << "explain"
                                                          << Document(explainInfo))));
}

RemoteCommandRequest DocumentSourceInternalSearchMongotRemote::getRemoteCommandRequest() const {
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
    return rcr;
}

BSONObj DocumentSourceInternalSearchMongotRemote::getExplainResponse() const {
    RemoteCommandRequest request = getRemoteCommandRequest();
    auto [promise, future] = makePromiseFuture<executor::TaskExecutor::RemoteCommandCallbackArgs>();
    auto promisePtr = std::make_shared<Promise<executor::TaskExecutor::RemoteCommandCallbackArgs>>(
        std::move(promise));
    auto scheduleResult = _taskExecutor->scheduleRemoteCommand(
        std::move(request), [promisePtr](const auto& args) { promisePtr->emplaceValue(args); });
    if (!scheduleResult.isOK()) {
        // Since the command failed to be scheduled, the callback above did not and will not run.
        // Thus, it is safe to fulfill the promise here without worrying about synchronizing access
        // with the executor's thread.
        promisePtr->setError(scheduleResult.getStatus());
    }
    auto response = future.getNoThrow(pExpCtx->opCtx);
    uassertStatusOK(response.getStatus());
    uassertStatusOK(response.getValue().response.status);
    BSONObj responseData = response.getValue().response.data;
    uassertStatusOK(getStatusFromCommandResult(responseData));
    auto explain = responseData["explain"];
    uassert(4895000,
            "Response must contain an 'explain' field that is of type 'Object'",
            explain.type() == BSONType::Object);
    return explain.embeddedObject().getOwned();
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
        _explainResponse = getExplainResponse();
        return DocumentSource::GetNextResult::makeEOF();
    }

    if (!_cursor) {
        _cursor.emplace(_taskExecutor, getRemoteCommandRequest());
    }

    auto response = _getNext();
    auto& opDebug = CurOp::get(pExpCtx->opCtx)->debug();

    if (opDebug.msWaitingForMongot) {
        *opDebug.msWaitingForMongot += durationCount<Milliseconds>(_cursor->resetWaitingTime());
    } else {
        opDebug.msWaitingForMongot = durationCount<Milliseconds>(_cursor->resetWaitingTime());
    }

    // Meta variables will be constant across the query and only need to be set once.
    if (!pExpCtx->variables.hasConstantValue(Variables::kSearchMetaId) && _cursor &&
        _cursor->getCursorVars() &&
        ::mongo::feature_flags::gFeatureFlagSearchMeta.isEnabled(
            serverGlobalParams.featureCompatibility)) {
        uassert(5858103,
                str::stream() << "Must enable 'featureFlagSearchMeta' to access '$$"
                              << Variables::getBuiltinVariableName(Variables::kSearchMetaId),
                ::mongo::feature_flags::gFeatureFlagSearchMeta.isEnabled(
                    serverGlobalParams.featureCompatibility));
        // Variables on the cursor must be an object.
        auto varsObj = Value(_cursor->getCursorVars().get());
        auto metaVal = varsObj.getDocument().getField(
            Variables::getBuiltinVariableName(Variables::kSearchMetaId));
        if (!metaVal.missing()) {
            uassert(5858100,
                    str::stream()
                        << "Search queries with collectors are not supported in sharded pipelines",
                    !pExpCtx->needsMerge);
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

BSONObj DocumentSourceInternalSearchMongotRemote::commandObject(
    const BSONObj& query, const intrusive_ptr<ExpressionContext>& expCtx) {
    BSONObjBuilder builder;
    builder.append("search", expCtx->ns.coll());
    expCtx->uuid.get().appendToBuilder(&builder, "collectionUUID");
    builder.append("query", query);
    if (expCtx->explain) {
        builder.append("explain",
                       BSON("verbosity" << ExplainOptions::verbosityString(*expCtx->explain)));
    }
    return builder.obj();
}

}  // namespace mongo
