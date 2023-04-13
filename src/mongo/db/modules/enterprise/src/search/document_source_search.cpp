/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_search.h"

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "lite_parsed_search.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongot_cursor.h"

namespace mongo {

using boost::intrusive_ptr;
using std::list;

REGISTER_DOCUMENT_SOURCE(search,
                         LiteParsedSearchStage::parse,
                         DocumentSourceSearch::createFromBson,
                         AllowedWithApiStrict::kNeverInVersion1);

// $searchBeta is supported as an alias for $search for compatibility with applications that used
// search during its beta period.
REGISTER_DOCUMENT_SOURCE(searchBeta,
                         LiteParsedSearchStage::parse,
                         DocumentSourceSearch::createFromBson,
                         AllowedWithApiStrict::kNeverInVersion1);

const char* DocumentSourceSearch::getSourceName() const {
    return kStageName.rawData();
}

Value DocumentSourceSearch::serialize(SerializationOptions opts) const {
    if (opts.redactIdentifiers || opts.replacementForLiteralArgs) {
        MONGO_UNIMPLEMENTED_TASSERT(7484366);
    }
    return Value(DOC(kStageName << Value(_userObj)));
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceSearch::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$search value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);
    auto specObj = elem.embeddedObject();

    // If we are parsing a view, we don't actually want to make any calls to mongot which happen
    // during desugaring.
    if (expCtx->isParsingViewDefinition) {
        return {make_intrusive<DocumentSourceSearch>(specObj, expCtx)};
    }

    std::list<intrusive_ptr<DocumentSource>> desugaredPipeline =
        mongot_cursor::createInitialSearchPipeline<DocumentSourceInternalSearchMongotRemote>(
            specObj, expCtx);

    // If 'returnStoredSource' is true, we don't want to do idLookup. Instead, promote the fields in
    // 'storedSource' to root.
    // 'getBoolField' returns false if the field is not present.
    if (specObj.getBoolField(kReturnStoredSourceArg)) {
        // {$replaceRoot: {newRoot: {$ifNull: ["$storedSource", "$$ROOT"]}}
        // 'storedSource' is not always present in the document from mongot. If it's present, use it
        // as the root. Otherwise keep the original document.
        BSONObj replaceRootSpec =
            BSON("$replaceRoot" << BSON(
                     "newRoot" << BSON(
                         "$ifNull" << BSON_ARRAY("$" + kProtocolStoredFieldsName << "$$ROOT"))));
        desugaredPipeline.push_back(
            DocumentSourceReplaceRoot::createFromBson(replaceRootSpec.firstElement(), expCtx));
    } else {
        // idLookup must always be immediately after the $mongotRemote stage, which is always first
        // in the pipeline.
        desugaredPipeline.insert(std::next(desugaredPipeline.begin()),
                                 make_intrusive<DocumentSourceInternalSearchIdLookUp>(expCtx));
    }

    return desugaredPipeline;
}
StageConstraints DocumentSourceSearch::constraints(Pipeline::SplitState pipeState) const {
    return DocumentSourceInternalSearchMongotRemote::getSearchDefaultConstraints();
}

}  // namespace mongo
