/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_search.h"

#include "document_source_internal_search_id_lookup.h"
#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_add_fields.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/expression_context.h"

namespace mongo {

using boost::intrusive_ptr;
using std::list;

REGISTER_DOCUMENT_SOURCE(search,
                         DocumentSourceSearch::LiteParsed::parse,
                         DocumentSourceSearch::createFromBson,
                         AllowedWithApiStrict::kNeverInVersion1);

// $searchBeta is supported as an alias for $search for compatibility with applications that used
// search during its beta period.
REGISTER_DOCUMENT_SOURCE(searchBeta,
                         DocumentSourceSearch::LiteParsed::parse,
                         DocumentSourceSearch::createFromBson,
                         AllowedWithApiStrict::kNeverInVersion1);

const char* DocumentSourceSearch::getSourceName() const {
    return kStageName.rawData();
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceSearch::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$search value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);
    // If 'returnStoredFields' is true, we don't want to do idLookup. Instead, promote the fields in
    // 'stored' to root.
    // 'getBoolField' returns false if the field is not present.
    if (elem.Obj().getBoolField(kReturnStoredFieldsArg)) {
        // {$replaceRoot: {newRoot: {$mergeObjects: [ {$ifNull: ["$stored", {}]}, {_id: "$_id"}]}
        // ]}}}. 'stored' is not always present in the document from mongot. If it's present, merge
        // it with _id in the return document. 'stored' may be missing if the user has not
        // configured their index to include any fields or if we are communicating with an older
        // mongot version that does not support this protocol.
        BSONObj replaceRootSpec = BSON(
            "$replaceRoot" << BSON(
                "newRoot" << BSON(
                    "$mergeObjects" << BSON_ARRAY(
                        BSON("$ifNull" << BSON_ARRAY("$" + kProtocolStoredFieldsName << BSONObj()))
                        << BSON("_id"
                                << "$_id")))));
        return {DocumentSourceInternalSearchMongotRemote::createFromBson(elem, pExpCtx),
                DocumentSourceReplaceRoot::createFromBson(replaceRootSpec.firstElement(), pExpCtx)};
    }
    return {
        DocumentSourceInternalSearchMongotRemote::createFromBson(elem, pExpCtx),
        new DocumentSourceInternalSearchIdLookUp(pExpCtx),
    };
}

}  // namespace mongo
