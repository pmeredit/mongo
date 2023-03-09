/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <queue>

#include "mongo/db/index/sort_key_generator.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_set_variable_from_subpipeline.h"
#include "mongo/db/pipeline/stage_constraints.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/stacktrace.h"

namespace mongo {

namespace search_constants {
// Default sort spec is to sort decreasing by search score.
const BSONObj kSortSpec = BSON("$searchScore" << -1);
constexpr auto kSearchSortValuesFieldPrefix = "$searchSortValues."_sd;
}  // namespace search_constants

/**
 * A class to retrieve $search results from a mongot process.
 */
class DocumentSourceInternalSearchMongotRemote : public DocumentSource {
public:
    static constexpr auto kReturnStoredSourceArg = "returnStoredSource"_sd;

    struct Params {
        Params(BSONObj queryObj)
            : query(queryObj), storedSource(queryObj.getBoolField(kReturnStoredSourceArg)) {}
        BSONObj query;
        boost::optional<int> protocolVersion;
        std::unique_ptr<Pipeline, PipelineDeleter> mergePipeline = nullptr;
        long long limit = 0;
        bool storedSource = false;
        boost::optional<BSONObj> sortSpec;
    };

    static constexpr StringData kStageName = "$_internalSearchMongotRemote"_sd;

    static Params parseParamsFromBson(BSONObj obj,
                                      const boost::intrusive_ptr<ExpressionContext>& expCtx);

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    static StageConstraints getSearchDefaultConstraints() {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kAllowed,
                                     UnionRequirement::kAllowed,
                                     ChangeStreamRequirement::kDenylist);
        constraints.requiresInputDocSource = false;
        return constraints;
    }

    /**
     * In a sharded environment this stage generates a DocumentSourceSetVariableFromSubPipeline to
     * run on the merging shard. This function contains the logic that allows that stage to move
     * past shards only stages.
     */
    static bool canMovePastDuringSplit(const DocumentSource& ds) {
        // Check if next stage uses the variable.
        std::set<Variables::Id> refs;
        ds.addVariableRefs(&refs);
        return !Variables::hasVariableReferenceTo(
                   refs, std::set<Variables::Id>{Variables::kSearchMetaId}) &&
            ds.constraints().preservesOrderAndMetadata;
    }


    DocumentSourceInternalSearchMongotRemote(Params&& params,
                                             const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                             executor::TaskExecutor* taskExecutor)
        : DocumentSource(kStageName, expCtx),
          _mergingPipeline(std::move(params.mergePipeline)),
          _searchQuery(params.query.getOwned()),
          _taskExecutor(taskExecutor),
          _storedSource(params.storedSource) {
        if (params.protocolVersion) {
            _metadataMergeProtocolVersion = *params.protocolVersion;
        }
        if (params.limit) {
            _limit = params.limit;
        }
        if (params.sortSpec.has_value()) {
            _sortSpec = params.sortSpec->getOwned();
            _sortKeyGen.emplace(SortPattern{*_sortSpec, pExpCtx}, pExpCtx->getCollator());
            // Verify that sortSpec do not contain dots after '$searchSortValues', as we expect it
            // to only contain top-level fields (no nested objects).
            for (auto&& k : *_sortSpec) {
                auto key = k.fieldNameStringData();
                if (key.startsWith(search_constants::kSearchSortValuesFieldPrefix)) {
                    key = key.substr(search_constants::kSearchSortValuesFieldPrefix.size());
                }
                tassert(
                    7320404,
                    "planShardedSearch returned sortSpec with key containing a dot: {}"_format(key),
                    key.find('.', 0) == std::string::npos);
            }
        }
    }

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        return getSearchDefaultConstraints();
    }

    const char* getSourceName() const override;

    /**
     * This is the first stage in the pipeline and so will always be run on shards. Mark as not
     * needing split as the sort can be deferred.
     */
    virtual boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        DistributedPlanLogic logic;

        logic.shardsStage = this;
        tassert(6448006, "Expected merging pipeline to be set already", _mergingPipeline);
        logic.mergingStages = {DocumentSourceSetVariableFromSubPipeline::create(
            pExpCtx, _mergingPipeline->clone(), Variables::kSearchMetaId)};
        logic.mergeSortPattern =
            _sortSpec.has_value() ? _sortSpec->getOwned() : search_constants::kSortSpec;
        logic.needsSplit = false;
        logic.canMovePast = canMovePastDuringSplit;

        return logic;
    }

    virtual boost::intrusive_ptr<DocumentSource> clone(
        const boost::intrusive_ptr<ExpressionContext>& newExpCtx) const override {
        auto params = Params{_searchQuery};
        params.protocolVersion = _metadataMergeProtocolVersion;
        params.mergePipeline = _mergingPipeline ? _mergingPipeline->clone(newExpCtx) : nullptr;
        params.storedSource = _storedSource;
        if (_sortSpec.has_value()) {
            params.sortSpec = _sortSpec->getOwned();
        }
        auto expCtx = newExpCtx ? newExpCtx : pExpCtx;
        return make_intrusive<DocumentSourceInternalSearchMongotRemote>(
            std::move(params), expCtx, _taskExecutor);
    }

    BSONObj getSearchQuery() const {
        return _searchQuery.getOwned();
    }

    auto getTaskExecutor() const {
        return _taskExecutor;
    }

    void setCursor(executor::TaskExecutorCursor cursor) {
        _cursor.emplace(std::move(cursor));
        _dispatchedQuery = true;
    }

    /**
     * If a cursor establishment phase was run and returned no documents, make sure we don't later
     * repeat the query to mongot.
     */
    void markCollectionEmpty() {
        _dispatchedQuery = true;
    }

    /**
     * Create a copy of this document source that can be given a different cursor from the original.
     * Copies everything necessary to make a mongot remote query, but does not copy the cursor.
     */
    boost::intrusive_ptr<DocumentSourceInternalSearchMongotRemote> copyForAlternateSource(
        executor::TaskExecutorCursor cursor,
        const boost::intrusive_ptr<ExpressionContext>& newExpCtx) {
        tassert(6635400, "newExpCtx should not be null", newExpCtx != nullptr);
        auto newStage = boost::intrusive_ptr<DocumentSourceInternalSearchMongotRemote>(
            static_cast<DocumentSourceInternalSearchMongotRemote*>(clone(newExpCtx).get()));
        newStage->setCursor(std::move(cursor));
        return newStage;
    }

    /**
     * Method to expose the status of the 'searchReturnsEoFImmediately' failpoint to the code
     * that sets up this document source.
     */
    static bool skipSearchStageRemoteSetup();

    boost::optional<int> getIntermediateResultsProtocolVersion() {
        // If it turns out that this stage is not running on a sharded collection, we don't want
        // to send the protocol version to mongot. If the protocol version is sent, mongot will
        // generate unmerged metadata documents that we won't be set up to merge.
        if (!pExpCtx->needsMerge) {
            return boost::none;
        }
        return _metadataMergeProtocolVersion;
    }

    void addVariableRefs(std::set<Variables::Id>* refs) const final {}

    auto isStoredSource() const {
        return _storedSource;
    }

protected:
    /**
     * Helper serialize method that avoids making mongot call during explain from mongos.
     */
    Document serializeWithoutMergePipeline(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const;

    virtual Value serialize(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const override;

    Value serialize(SerializationOptions opts) const override {
        MONGO_UNIMPLEMENTED;
    }

    /**
     * Inspects the cursor to see if it set any vars, and propogates their definitions to the
     * ExpressionContext. For now, we only expect SEARCH_META to be defined.
     */
    void tryToSetSearchMetaVar();

    virtual executor::TaskExecutorCursor establishCursor();

    virtual GetNextResult getNextAfterSetup();

    bool shouldReturnEOF();

    Pipeline::SourceContainer::iterator doOptimizeAt(Pipeline::SourceContainer::iterator itr,
                                                     Pipeline::SourceContainer* container) override;

    /**
     * This stage may need to merge the metadata it generates on the merging half of the pipeline.
     * Until we know if the merge needs to be done, we hold the pipeline containig the merging
     * logic here.
     */
    std::unique_ptr<Pipeline, PipelineDeleter> _mergingPipeline;

private:
    /**
     * Does some common setup and checks, then calls 'getNextAfterSetup()' if appropriate.
     */
    GetNextResult doGetNext() final;

    boost::optional<BSONObj> _getNext();

    const BSONObj _searchQuery;

    // If this is an explain of a $search at execution-level verbosity, then the explain
    // results are held here. Otherwise, this is an empty object.
    BSONObj _explainResponse;

    executor::TaskExecutor* _taskExecutor;

    boost::optional<executor::TaskExecutorCursor> _cursor;

    /**
     * Track whether either the stage or an earlier caller issues a mongot remote request. This
     * can be true even if '_cursor' is boost::none, which can happen if no documents are returned.
     */
    bool _dispatchedQuery = false;

    // Store the cursorId. We need to store it on the document source because the id on the
    // TaskExecutorCursor will be set to zero after the final getMore after the cursor is
    // exhausted.
    boost::optional<CursorId> _cursorId{boost::none};

    /**
     * Protocol version if it must be communicated via the search request.
     * If we are in a sharded environment but on a non-sharded collection we may have a protocol
     * version even though it should not be sent to mongot.
     */
    boost::optional<int> _metadataMergeProtocolVersion;

    unsigned long long _limit = 0;
    unsigned long long _docsReturned = 0;

    bool _storedSource = false;

    /**
     * Sort specification for the current query. Used to populate the $sortKey on mongod after
     * documents are returned from mongot.
     * boost::none if plan sharded search did not specify a sort.
     */
    boost::optional<BSONObj> _sortSpec;

    /**
     * Sort key generator used to populate $sortKey. Has a value iff '_sortSpec' has a value.
     */
    boost::optional<SortKeyGenerator> _sortKeyGen;
};

namespace search_meta {
/**
 * This function walks the pipeline and verifies that if there is a $search stage in a sub-pipeline
 * that there is no $$SEARCH_META access.
 */
void assertSearchMetaAccessValid(const Pipeline::SourceContainer& pipeline);

}  // namespace search_meta
}  // namespace mongo
