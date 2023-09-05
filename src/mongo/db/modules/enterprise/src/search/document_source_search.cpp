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
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_replace_root.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/skip_and_limit.h"
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

Value DocumentSourceSearch::serialize(const SerializationOptions& opts) const {
    if (!opts.verbosity || pExpCtx->inMongos) {
        if (_spec) {
            MutableDocument spec{Document(_spec->toBSON())};
            // In a non-sharded scenario we don't need to pass the limit around as the limit stage
            // will do equivalent work. In a sharded scenario we want the limit to get to the
            // shards, so we serialize it. We serialize it in this block as all sharded search
            // queries have a protocol version.
            // This is the limit that we copied, and does not replace the real limit stage later in
            // the pipeline.
            if (_limit) {
                spec.addField(InternalSearchMongotRemoteSpec::kLimitFieldName,
                              opts.serializeLiteral(_limit.value()));
            }
            return Value(Document{{getSourceName(), spec.freezeToValue()}});
        }
    }
    return Value(DOC(getSourceName() << opts.serializeLiteral(_searchQuery)));
}

intrusive_ptr<DocumentSource> DocumentSourceSearch::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "$search value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);
    auto specObj = elem.embeddedObject();

    // If kMongotQueryFieldName is present, this is the case that we re-create the DocumentSource
    // from a serialized DocumentSourceSearch that was originally parsed on a router.
    if (specObj.hasField(InternalSearchMongotRemoteSpec::kMongotQueryFieldName)) {
        boost::optional<long long> limit =
            specObj.hasField(InternalSearchMongotRemoteSpec::kLimitFieldName)
            ? boost::optional<long long>(
                  specObj.getField(InternalSearchMongotRemoteSpec::kLimitFieldName).numberLong())
            : boost::none;

        return make_intrusive<DocumentSourceSearch>(
            specObj.getField(InternalSearchMongotRemoteSpec::kMongotQueryFieldName).Obj(),
            expCtx,
            InternalSearchMongotRemoteSpec::parse(IDLParserContext(kStageName), specObj),
            limit);
    } else {
        return make_intrusive<DocumentSourceSearch>(specObj, expCtx, boost::none, boost::none);
    }
}


std::list<intrusive_ptr<DocumentSource>> DocumentSourceSearch::desugar() {
    auto executor = executor::getMongotTaskExecutor(pExpCtx->opCtx->getServiceContext());
    std::list<intrusive_ptr<DocumentSource>> desugaredPipeline;

    bool storedSource = _searchQuery.getBoolField(kReturnStoredSourceArg);

    if (_spec) {
        auto spec = InternalSearchMongotRemoteSpec::parseOwned(IDLParserContext(kStageName),
                                                               _spec->toBSON());

        // Pass the limit in when there is no idLookup stage.
        // TODO: SERVER-76591 remove limit after task done.
        spec.setLimit(storedSource && _limit ? _limit.value() : 0);
        // Remove mergingPipeline info since it is not useful for
        // DocumentSourceInternalSearchMongotRemote.
        spec.setMergingPipeline(boost::none);

        desugaredPipeline.push_back(make_intrusive<DocumentSourceInternalSearchMongotRemote>(
            spec, pExpCtx, executor, _limit));
    } else {
        desugaredPipeline.push_back(make_intrusive<DocumentSourceInternalSearchMongotRemote>(
            _searchQuery, pExpCtx, executor, _limit));
    }

    // If 'returnStoredSource' is true, we don't want to do idLookup. Instead, promote the fields in
    // 'storedSource' to root.
    // 'getBoolField' returns false if the field is not present.
    if (storedSource) {
        // {$replaceRoot: {newRoot: {$ifNull: ["$storedSource", "$$ROOT"]}}
        // 'storedSource' is not always present in the document from mongot. If it's present, use it
        // as the root. Otherwise keep the original document.
        BSONObj replaceRootSpec =
            BSON("$replaceRoot" << BSON(
                     "newRoot" << BSON(
                         "$ifNull" << BSON_ARRAY("$" + kProtocolStoredFieldsName << "$$ROOT"))));
        desugaredPipeline.push_back(
            DocumentSourceReplaceRoot::createFromBson(replaceRootSpec.firstElement(), pExpCtx));
    } else {
        // idLookup must always be immediately after the $mongotRemote stage, which is always first
        // in the pipeline.
        desugaredPipeline.insert(
            std::next(desugaredPipeline.begin()),
            make_intrusive<DocumentSourceInternalSearchIdLookUp>(pExpCtx, _limit.value_or(0)));
    }

    return desugaredPipeline;
}

StageConstraints DocumentSourceSearch::constraints(Pipeline::SplitState pipeState) const {
    return DocumentSourceInternalSearchMongotRemote::getSearchDefaultConstraints();
}

Pipeline::SourceContainer::iterator DocumentSourceSearch::doOptimizeAt(
    Pipeline::SourceContainer::iterator itr, Pipeline::SourceContainer* container) {
    // In the case where the query has an extractable limit, we send that limit to mongot as a guide
    // for the number of documents mongot should return (rather than the default batchsize).
    // Move past the current stage ($search).
    auto stageItr = std::next(itr);
    // Only attempt to get the limit from the query if there are further stages in the pipeline.
    if (stageItr != container->end()) {
        // Calculate the extracted limit without modifying the rest of the pipeline.
        _limit = getUserLimit(stageItr, container);
    }

    // Determine whether the pipeline references the $$SEARCH_META variable. We won't insert a
    // $setVariableFromSubPipeline stage until we split the pipeline (see distributedPlanLogic()),
    // but at that point we don't have access to the full pipeline to know whether we need it.
    _pipelineNeedsSearchMeta = std::any_of(std::next(itr), container->end(), [](const auto& itr) {
        return mongot_cursor::hasReferenceToSearchMeta(*itr);
    });

    return std::next(itr);
}

void DocumentSourceSearch::validateSortSpec(boost::optional<BSONObj> sortSpec) {
    if (sortSpec) {
        // Verify that sortSpec do not contain dots after '$searchSortValues', as we expect it
        // to only contain top-level fields (no nested objects).
        for (auto&& k : *sortSpec) {
            auto key = k.fieldNameStringData();
            if (key.startsWith(mongot_cursor::kSearchSortValuesFieldPrefix)) {
                key = key.substr(mongot_cursor::kSearchSortValuesFieldPrefix.size());
            }
            tassert(7320404,
                    "planShardedSearch returned sortSpec with key containing a dot: {}"_format(key),
                    key.find('.', 0) == std::string::npos);
        }
    }
}

boost::optional<DocumentSource::DistributedPlanLogic> DocumentSourceSearch::distributedPlanLogic() {
    if (!_spec) {
        // Issue a planShardedSearch call to mongot if we have not done yet, and validate the
        // sortSpec in response.
        _spec = mongot_cursor::planShardedSearch(pExpCtx, _searchQuery);
        validateSortSpec(_spec->getSortSpec());
    }

    // Construct the DistributedPlanLogic for sharded planning based on the information returned
    // from mongot.
    DistributedPlanLogic logic;
    logic.shardsStage = this;
    if (_spec->getMergingPipeline() && _pipelineNeedsSearchMeta) {
        logic.mergingStages = {DocumentSourceSetVariableFromSubPipeline::create(
            pExpCtx,
            Pipeline::parse(*_spec->getMergingPipeline(), pExpCtx),
            Variables::kSearchMetaId)};
    }

    logic.mergeSortPattern = _spec->getSortSpec().has_value() ? _spec->getSortSpec()->getOwned()
                                                              : mongot_cursor::kSortSpec;

    logic.needsSplit = false;
    logic.canMovePast = canMovePastDuringSplit;

    return logic;
}

}  // namespace mongo
