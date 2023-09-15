/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#include "document_source_search_meta.h"

#include "document_source_internal_search_mongot_remote.h"
#include "lite_parsed_search.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/query/cursor_response_gen.h"
#include "mongot_cursor.h"
#include "search_task_executors.h"

namespace mongo {

using boost::intrusive_ptr;
using std::list;

REGISTER_DOCUMENT_SOURCE_CONDITIONALLY(searchMeta,
                                       LiteParsedSearchStage::parse,
                                       DocumentSourceSearchMeta::createFromBson,
                                       AllowedWithApiStrict::kNeverInVersion1,
                                       AllowedWithClientType::kAny,
                                       boost::none,
                                       true);

Value DocumentSourceSearchMeta::serialize(const SerializationOptions& opts) const {
    if (!pExpCtx->explain && pExpCtx->inMongos) {
        return Value(Document{{getSourceName(), serializeWithoutMergePipeline(opts)}});
    }
    return DocumentSourceInternalSearchMongotRemote::serialize(opts);
}

executor::TaskExecutorCursor DocumentSourceSearchMeta::establishCursor() {
    auto cursors = mongot_cursor::establishSearchCursors(pExpCtx,
                                                         getSearchQuery(),
                                                         getTaskExecutor(),
                                                         getMongotDocsRequested(),
                                                         nullptr /* augmentGetMore */,
                                                         getIntermediateResultsProtocolVersion());
    if (cursors.size() == 1) {
        const auto& cursor = *cursors.begin();
        tassert(6448010,
                "If there's one cursor we expect to get SEARCH_META from the attached vars",
                !getIntermediateResultsProtocolVersion() && !cursor.getType() &&
                    cursor.getCursorVars());
        return std::move(*cursors.begin());
    }
    for (auto&& cursor : cursors) {
        tassert(6448008, "Expected every mongot cursor to come back with a type", cursor.getType());
        auto cursorType = CursorType_parse(IDLParserContext("ShardedAggHelperCursorType"),
                                           cursor.getType().value());
        if (cursorType == CursorTypeEnum::SearchMetaResult) {
            // Note this may leak the other cursor(s). Should look into whether we can killCursors.
            return std::move(cursor);
        }
    }
    tasserted(6448009, "Expected to get a metadata cursor back from mongot, found none");
}

DocumentSource::GetNextResult DocumentSourceSearchMeta::getNextAfterSetup() {
    if (pExpCtx->needsMerge) {
        // When we are merging $searchMeta we have established a cursor which only returns metadata
        // results (see 'establishCursor()'). So just iterate that cursor normally.
        return DocumentSourceInternalSearchMongotRemote::getNextAfterSetup();
    }

    if (!_returnedAlready) {
        tryToSetSearchMetaVar();
        auto& vars = pExpCtx->variables;
        tassert(6448005,
                "Expected SEARCH_META to be set for $searchMeta stage",
                vars.hasConstantValue(Variables::kSearchMetaId) &&
                    vars.getValue(Variables::kSearchMetaId).isObject());
        _returnedAlready = true;
        return {vars.getValue(Variables::kSearchMetaId).getDocument()};
    }
    return GetNextResult::makeEOF();
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceSearchMeta::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$searchMeta value must be an object. Found: "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object);

    auto specObj = elem.embeddedObject();

    // Note that the $searchMeta stage has two parsing options: one for the user visible stage and
    // the second (longer) form which is serialized from mongos to the shards and includes more
    // information such as merging pipeline.

    // Avoid any calls to mongot during desugaring.
    if (expCtx->isParsingViewDefinition) {
        auto executor = executor::getMongotTaskExecutor(expCtx->opCtx->getServiceContext());
        return {make_intrusive<DocumentSourceSearchMeta>(specObj.getOwned(), expCtx, executor)};
    }

    if (expCtx->needsMerge) {
        // If we need to merge output later, we just need to produce this shard's metadata and
        // that's it. Expect to parse the long form.
        auto params = InternalSearchMongotRemoteSpec::parse(IDLParserContext(kStageName), specObj);
        auto executor = executor::getMongotTaskExecutor(expCtx->opCtx->getServiceContext());
        return {make_intrusive<DocumentSourceSearchMeta>(std::move(params), expCtx, executor)};
    }

    // Otherwise, we need to call this helper to determine if this is a sharded environment. If so,
    // we need to consult a mongot to construct such a merging pipeline for us to use later.
    return mongot_cursor::createInitialSearchPipeline<DocumentSourceSearchMeta>(specObj, expCtx);
}

}  // namespace mongo
