/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/document_source_remote_db_cursor.h"

#include <boost/optional/optional.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <fmt/format.h>

#include "mongo/bson/bsontypes.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/query/getmore_command_gen.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_component.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/uuid.h"
#include "streams/exec/mongocxx_utils.h"

namespace streams {
using boost::intrusive_ptr;
using namespace mongo;

const char* DocumentSourceRemoteDbCursor::getSourceName() const {
    return kStageName.rawData();
}

void DocumentSourceRemoteDbCursor::doDispose() {}

DocumentSource::GetNextResult DocumentSourceRemoteDbCursor::doGetNext() {
    using namespace fmt::literals;

    while (_batchIter != _batch.cend() || _cursorId != 0) {
        // First, tries to exhaust the current batch.
        if (_batchIter != _batch.cend()) {
            auto doc = _batchIter->Obj();
            ++_batchIter;
            return GetNextResult{Document(doc)};
        }

        // The current batch is exhausted, but the '_cursorId' indicates that there are no more data
        // and so breaks out of the loop.
        if (_cursorId == 0) {
            break;
        }

        // If the current batch is exhausted, tries to get the next batch.
        GetMoreCommandRequest request(_cursorId, pExpCtx->getNamespaceString().coll().toString());
        request.setDbName(pExpCtx->getNamespaceString().dbName());
        request.setBatchSize(kDefaultBatchSize);
        _reply = _procItf->runCommand(request);

        /*
         * The next response looks like this:
         * {
         *   "cursor" : {
         *      "nextBatch" : [
         *        {
         *          "_id" : ObjectId("6568f53b24e2af25238ef394"),
         *          "a" : 2
         *        }
         *      ],
         *      "id" : NumberLong("3254987687720798413"),
         *      "ns" : "test.a"
         *   },
         *   "ok" : 1
         * }
         *
         * The last response looks like this:
         * {
         *   "cursor" : {
         *      "nextBatch" : [ ],
         *      "id" : NumberLong(0),
         *      "ns" : "test.a"
         *   },
         *   "ok" : 1
         * }
         */
        tassert(8369603,
                "'cursor' field absent or not an object",
                _reply.hasField("cursor") && _reply["cursor"].isABSONObj());
        auto cursor = _reply["cursor"].embeddedObject();
        tassert(
            8369604,
            "'cursor' field missing '{}' field"_format(GetMoreResponseCursor::kCursorIdFieldName),
            cursor.hasField(GetMoreResponseCursor::kCursorIdFieldName) &&
                cursor[GetMoreResponseCursor::kCursorIdFieldName].type() == BSONType::NumberLong);
        _cursorId = cursor[GetMoreResponseCursor::kCursorIdFieldName].numberLong();
        tassert(
            8369605,
            "'cursor' field missing '{}' field"_format(GetMoreResponseCursor::kNextBatchFieldName),
            cursor.hasField(GetMoreResponseCursor::kNextBatchFieldName) &&
                cursor[GetMoreResponseCursor::kNextBatchFieldName].type() == BSONType::Array);
        _batch = cursor[GetMoreResponseCursor::kNextBatchFieldName].Array();
        _batchIter = _batch.cbegin();
    }

    return GetNextResult::makeEOF();
}

DocumentSourceRemoteDbCursor::~DocumentSourceRemoteDbCursor() {}

DocumentSourceRemoteDbCursor::DocumentSourceRemoteDbCursor(MongoDBProcessInterface* procItf,
                                                           const Pipeline* pipeline)
    : DocumentSource(kStageName, pipeline->getContext()), _procItf(procItf) {
    using namespace fmt::literals;

    AggregateCommandRequest request(pExpCtx->getNamespaceString(), pipeline->serializeToBson());
    SimpleCursorOptions cursorOptions;
    cursorOptions.setBatchSize(kDefaultBatchSize);
    request.setCursor(cursorOptions);

    // If there are defined variables, then adds them to the request. While building the 'pipeline',
    // 'variables' have been resolved to actual constant values and so we can just add them to the
    // request.
    if (auto variablesParseState = pipeline->getContext()->variablesParseState;
        variablesParseState.hasDefinedVariables()) {
        auto varsObj = variablesParseState.serialize(pipeline->getContext()->variables);
        request.setLet({varsObj});
    }

    _reply = _procItf->runCommand(request);

    /*
     * The initial response looks like this:
     * {
     *   "cursor" : {
     *     "firstBatch" : [
     *       {
     *         "_id" : ObjectId("6568f53b24e2af25238ef393"),
     *         "a" : 1
     *       }
     *     ],
     *     "id" : NumberLong("3254987687720798413"),
     *     "ns" : "test.a"
     *   },
     *   "ok" : 1
     * }
     */
    tassert(8369600,
            "'cursor' field absent or not an object",
            _reply.hasField("cursor") && _reply["cursor"].isABSONObj());
    auto cursor = _reply["cursor"].embeddedObject();
    tassert(8369601,
            "'cursor' field missing '{}' field"_format(InitialResponseCursor::kCursorIdFieldName),
            cursor.hasField(InitialResponseCursor::kCursorIdFieldName) &&
                cursor[InitialResponseCursor::kCursorIdFieldName].type() == BSONType::NumberLong);
    _cursorId = cursor[InitialResponseCursor::kCursorIdFieldName].numberLong();
    tassert(8369602,
            "'cursor' field missing '{}' field"_format(InitialResponseCursor::kFirstBatchFieldName),
            cursor.hasField(InitialResponseCursor::kFirstBatchFieldName) &&
                cursor[InitialResponseCursor::kFirstBatchFieldName].type() == BSONType::Array);
    _batch = cursor[InitialResponseCursor::kFirstBatchFieldName].Array();
    _batchIter = _batch.cbegin();
}

intrusive_ptr<DocumentSourceRemoteDbCursor> DocumentSourceRemoteDbCursor::create(
    MongoDBProcessInterface* processInterface, const Pipeline* pipeline) {
    intrusive_ptr<DocumentSourceRemoteDbCursor> source{
        new DocumentSourceRemoteDbCursor(processInterface, pipeline)};
    return source;
}
}  // namespace streams
