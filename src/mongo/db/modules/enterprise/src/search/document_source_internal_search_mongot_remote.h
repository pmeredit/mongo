/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <queue>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_set_variable_from_subpipeline.h"
#include "mongo/db/pipeline/stage_constraints.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/stacktrace.h"

namespace mongo {

namespace search_constants {
const BSONObj kSortSpec = BSON("$searchScore" << -1);

}  // namespace search_constants

/**
 * A class to retrieve $search results from a mongot process.
 *
 * Work slated and not handled yet:
 * - TODO Handle sharded sort merging properly (SERVER-40015)
 */
class DocumentSourceInternalSearchMongotRemote final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$_internalSearchMongotRemote"_sd;

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const NamespaceString& nss,
                                                 const BSONElement& spec) {
            return std::make_unique<LiteParsed>(spec.fieldName(), nss);
        }

        explicit LiteParsed(std::string parseTimeName, NamespaceString nss)
            : LiteParsedDocumentSource(std::move(parseTimeName)), _nss(std::move(nss)) {}

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos,
                                           bool bypassDocumentValidation) const final {
            return {Privilege(ResourcePattern::forExactNamespace(_nss), ActionType::find)};
        }

        bool isInitialSource() const final {
            return true;
        }

    private:
        const NamespaceString _nss;
    };

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
        DepsTracker depsTracker;
        bool depTrackingSupported = ds.getDependencies(&depsTracker) != DepsTracker::NOT_SUPPORTED;
        // Check if next stage uses the variable.
        return depTrackingSupported &&
            !depsTracker.hasVariableReferenceTo(
                std::set<Variables::Id>{Variables::kSearchMetaId}) &&
            ds.constraints().preservesOrderAndMetadata;
    }

    DocumentSourceInternalSearchMongotRemote(
        const BSONObj& query,
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        executor::TaskExecutor* executor,
        int protocolVersion,
        std::unique_ptr<Pipeline, PipelineDeleter> mergePipeline);

    virtual ~DocumentSourceInternalSearchMongotRemote() = default;

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        return getSearchDefaultConstraints();
    }

    const char* getSourceName() const override;

    /**
     * This is the first stage in the pipeline and so will always be run on shards. Mark as not
     * needing split as the sort can be deferred.
     */
    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        DistributedPlanLogic logic;

        logic.shardsStage = this;
        if (::mongo::feature_flags::gFeatureFlagSearchShardedFacets.isEnabled(
                serverGlobalParams.featureCompatibility)) {
            logic.mergingStage = DocumentSourceSetVariableFromSubPipeline::create(
                pExpCtx, _mergingPipeline->clone(), Variables::kSearchMetaId);
        }
        logic.mergeSortPattern = search_constants::kSortSpec;
        logic.needsSplit = false;
        logic.canMovePast = canMovePastDuringSplit;

        return logic;
    }

    virtual boost::intrusive_ptr<DocumentSource> clone() const override {
        if (_metadataMergeProtocolVersion) {
            return new DocumentSourceInternalSearchMongotRemote(
                _searchQuery,
                pExpCtx,
                _taskExecutor,
                _metadataMergeProtocolVersion.get(),
                _mergingPipeline ? _mergingPipeline->clone() : nullptr);
        }
        return new DocumentSourceInternalSearchMongotRemote(_searchQuery, pExpCtx, _taskExecutor);
    }

    Value serialize(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const override;

    BSONObj getSearchQuery() {
        return _searchQuery.getOwned();
    }

    auto getTaskExecutor() {
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
        executor::TaskExecutorCursor cursor) {
        auto newStage =
            new DocumentSourceInternalSearchMongotRemote(_searchQuery, pExpCtx, _taskExecutor);
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

private:
    DocumentSourceInternalSearchMongotRemote(const BSONObj& query,
                                             const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                             executor::TaskExecutor* taskExecutor)
        : DocumentSource(kStageName, expCtx),
          _searchQuery(query.getOwned()),
          _taskExecutor(taskExecutor) {}

    boost::optional<BSONObj> _getNext();

    GetNextResult doGetNext() override;

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

    /**
     * This stage may need to merge the metadata it generates on the merging half of the pipeline.
     * Until we know if the merge needs to be done, we hold the pipeline containig the merging
     * logic here.
     */
    std::unique_ptr<Pipeline, PipelineDeleter> _mergingPipeline;
};

namespace search_meta {
/**
 * This function walks the pipeline and verifies that if there is a $search stage in a sub-pipeline
 * that there is no $$SEARCH_META access.
 */
void assertSearchMetaAccessValid(const Pipeline::SourceContainer& pipeline);

}  // namespace search_meta
}  // namespace mongo
