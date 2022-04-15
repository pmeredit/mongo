/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#include "document_source_search_meta.h"

#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_documents.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_mock_collection.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/document_source_set_variable_from_subpipeline.h"
#include "mongo/db/pipeline/document_source_union_with.h"
#include "mongo/db/pipeline/expression.h"
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

    // To compute '$searchMeta' we essentially run a $search stage and throw out the "normal"
    // results and just keep the metadata. Unfortunately it's not that simple because the pipeline
    // needs to return metadata even if there were no search results. The $search stage doesn't have
    // this behavior, so instead we'll do this trick:
    // [
    //   {$documents: [{}]},  // Generate a single document which will be the result.
    //   {$setVariableFromSubPipeline: {
    //      varId: kSearchMetaId,
    //      pipeline: [
    //        {$search: {/* search query */}},
    //        {$replaceWith: "$$SEARCH_META"},
    //        {$limit: 1}
    //      ],
    //      // This argument will handle the case with no search results.
    //      // The variable is still defined in this case.
    //      ifEmpty: "$$SEARCH_META"
    //   }},
    //   // Now that we've computed SEARCH_META, replace our dummy document with the value.
    //   {$replaceWith: "$$SEARCH_META"}
    // ]

    std::list<intrusive_ptr<DocumentSource>> output;
    // A single placeholder document to be returned.
    // In order to use $documents we normally have to run a collectionless aggregate. We don't want
    // to change the ExpressionContext's namespace here for fear of confusing other stages in the
    // pipeline or debugging information, so instead we'll use $mockCollection which relaxes this
    // constraint.
    output.emplace_back(make_intrusive<DocumentSourceMockCollection>(
        std::deque<DocumentSource::GetNextResult>{Document{}}, expCtx));
    // Then a pipeline to set the SEARCH_META variable.
    auto subPipelineCtx = expCtx->copyForSubPipeline(expCtx->ns, expCtx->uuid);
    auto setVarPipeline = Pipeline::create(
        [&]() {
            auto setVarPipeline = mongot_cursor::createInitialSearchPipeline(elem, subPipelineCtx);
            setVarPipeline.emplace_back(DocumentSourceReplaceRoot::createFromBson(
                BSON("$replaceRoot" << BSON("newRoot" << std::string("$$SEARCH_META")))
                    .firstElement(),
                subPipelineCtx));
            setVarPipeline.emplace_back(DocumentSourceLimit::create(subPipelineCtx, 1));
            return setVarPipeline;
        }(),
        subPipelineCtx);
    output.emplace_back(DocumentSourceSetVariableFromSubPipeline::create(
        expCtx,
        subPipelineCtx->mongoProcessInterface->attachCursorSourceToPipeline(
            setVarPipeline.release()),
        Variables::kSearchMetaId,
        ExpressionFieldPath::createVarFromString(
            subPipelineCtx.get(), "SEARCH_META", subPipelineCtx->variablesParseState)));
    output.emplace_back(DocumentSourceReplaceRoot::createFromBson(
        BSON("$replaceRoot" << BSON("newRoot" << std::string("$$SEARCH_META"))).firstElement(),
        expCtx));
    return output;
}

StageConstraints DocumentSourceSearchMeta::constraints(Pipeline::SplitState pipeState) const {
    return DocumentSourceInternalSearchMongotRemote::getSearchDefaultConstraints();
}

Value DocumentSourceSearchMeta::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(kStageName << Value(_userObj)));
}

}  // namespace mongo
