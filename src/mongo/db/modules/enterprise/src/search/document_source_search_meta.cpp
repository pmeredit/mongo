/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#include "document_source_search_meta.h"

#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_documents.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_union_with.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"

namespace mongo {

using boost::intrusive_ptr;
using std::list;

REGISTER_DOCUMENT_SOURCE_CONDITIONALLY(
    searchMeta,
    DocumentSourceSearchMeta::LiteParsed::parse,
    DocumentSourceSearchMeta::createFromBson,
    AllowedWithApiStrict::kNeverInVersion1,
    AllowedWithClientType::kAny,
    boost::none,
    ::mongo::feature_flags::gFeatureFlagSearchMeta.isEnabledAndIgnoreFCV());

const char* DocumentSourceSearchMeta::getSourceName() const {
    return kStageName.rawData();
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceSearchMeta::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {

    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$searchMeta value must be an object. Found: "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object);

    std::list<intrusive_ptr<DocumentSource>> res{
        // Fetch the search results and populate $$SEARCH_META.
        DocumentSourceInternalSearchMongotRemote::createFromBson(elem, pExpCtx),
        // Replace any documents returned by mongot with the SEARCH_META contents.
        DocumentSourceReplaceRoot::createFromBson(
            BSON("$replaceRoot" << BSON("newRoot" << std::string("$$SEARCH_META"))).firstElement(),
            pExpCtx),
        // If mongot returned no documents, generate a document to return.
        DocumentSourceUnionWith::createFromBson(
            BSON(DocumentSourceUnionWith::kStageName
                 << BSON("pipeline" << BSON_ARRAY(BSON(DocumentSourceDocuments::kStageName
                                                       << BSON_ARRAY("$SEARCH_META")))))
                .firstElement(),
            pExpCtx.get()),
        // Only return one copy of the meta results.
        DocumentSourceLimit::create(pExpCtx, 1)};
    return res;
}

}  // namespace mongo
