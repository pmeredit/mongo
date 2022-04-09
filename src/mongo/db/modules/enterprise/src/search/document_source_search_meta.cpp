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
#include "mongot_cursor.h"

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
    feature_flags::gFeatureFlagSearchMeta.isEnabledAndIgnoreFCV());

const char* DocumentSourceSearchMeta::getSourceName() const {
    return kStageName.rawData();
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceSearchMeta::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {

    // If we are parsing a view, we don't actually want to make any calls to mongot which happen
    // during desugaring.
    if (expCtx->isParsingViewDefinition) {
        uassert(ErrorCodes::FailedToParse,
                str::stream() << "$searchMeta value must be an object. Found: "
                              << typeName(elem.type()),
                elem.type() == BSONType::Object);
        intrusive_ptr<DocumentSourceSearchMeta> source(
            new DocumentSourceSearchMeta(elem.Obj().getOwned(), expCtx));
        return {source};
    }

    std::list<intrusive_ptr<DocumentSource>> desugaredPipeline =
        mongot_cursor::createInitialSearchPipeline(elem, expCtx);

    // Do not send search query results to merging node.
    desugaredPipeline.push_back(DocumentSourceLimit::create(expCtx, 1));

    // Replace any documents returned by mongot with the SEARCH_META contents.
    desugaredPipeline.push_back(DocumentSourceReplaceRoot::createFromBson(
        BSON("$replaceRoot" << BSON("newRoot" << std::string("$$SEARCH_META"))).firstElement(),
        expCtx));

    // If mongot returned no documents, generate a document to return. This depends on the
    // current, undocumented behavior of $unionWith that the outer pipeline is iterated before
    // the inner pipeline.
    desugaredPipeline.push_back(DocumentSourceUnionWith::createFromBson(
        BSON(DocumentSourceUnionWith::kStageName
             << BSON("pipeline" << BSON_ARRAY(
                         BSON(DocumentSourceDocuments::kStageName << BSON_ARRAY("$$SEARCH_META")))))
            .firstElement(),
        expCtx.get()));

    // Only return one copy of the meta results.
    desugaredPipeline.push_back(DocumentSourceLimit::create(expCtx, 1));

    return desugaredPipeline;
}

StageConstraints DocumentSourceSearchMeta::constraints(Pipeline::SplitState pipeState) const {
    return DocumentSourceInternalSearchMongotRemote::getSearchDefaultConstraints();
}

Value DocumentSourceSearchMeta::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(kStageName << Value(_userObj)));
}

}  // namespace mongo
