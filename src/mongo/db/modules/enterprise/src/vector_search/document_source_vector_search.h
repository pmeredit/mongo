/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/executor/task_executor_cursor.h"
#include "vector_search/document_source_vector_search_gen.h"

namespace mongo {

/**
 * A class to retrieve vector search results from a mongot process.
 */
class DocumentSourceVectorSearch : public DocumentSource {
public:
    const BSONObj kSortSpec = BSON("$vectorSearchScore" << -1);
    static constexpr StringData kStageName = "$vectorSearch"_sd;

    DocumentSourceVectorSearch(VectorSearchSpec&& request,
                               const boost::intrusive_ptr<ExpressionContext>& expCtx,
                               std::shared_ptr<executor::TaskExecutor> taskExecutor);

    static std::list<boost::intrusive_ptr<DocumentSource>> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    const char* getSourceName() const override {
        return kStageName.rawData();
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        DistributedPlanLogic logic;
        logic.shardsStage = this;
        logic.mergingStages = {DocumentSourceLimit::create(pExpCtx, _limit)};
        logic.mergeSortPattern = kSortSpec;
        return logic;
    }

    void addVariableRefs(std::set<Variables::Id>* refs) const final {}

    boost::intrusive_ptr<DocumentSource> clone(
        const boost::intrusive_ptr<ExpressionContext>& newExpCtx) const override {
        auto expCtx = newExpCtx ? newExpCtx : pExpCtx;
        return make_intrusive<DocumentSourceVectorSearch>(
            VectorSearchSpec(_request), expCtx, _taskExecutor);
    }

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kNotAllowed,
                                     UnionRequirement::kNotAllowed,
                                     ChangeStreamRequirement::kDenylist);
        constraints.requiresInputDocSource = false;
        return constraints;
    };

protected:
    Value serialize(const SerializationOptions& opts) const override;

    Pipeline::SourceContainer::iterator doOptimizeAt(Pipeline::SourceContainer::iterator itr,
                                                     Pipeline::SourceContainer* container) override;

private:
    // Get the next record from mongot. This will establish the mongot cursor on the first call.
    GetNextResult doGetNext() final;

    boost::optional<BSONObj> getNext();

    DocumentSource::GetNextResult getNextAfterSetup();

    // If this is an explain of a $vectorSearch at execution-level verbosity, then the explain
    // results are held here. Otherwise, this is an empty object.
    BSONObj _explainResponse;

    const VectorSearchSpec _request;

    const std::unique_ptr<MatchExpression> _filterExpr;

    std::shared_ptr<executor::TaskExecutor> _taskExecutor;

    boost::optional<executor::TaskExecutorCursor> _cursor;

    // Store the cursorId. We need to store it on the document source because the id on the
    // TaskExecutorCursor will be set to zero after the final getMore after the cursor is
    // exhausted.
    boost::optional<CursorId> _cursorId{boost::none};

    // Limit value for the pipeline as a whole. This is not the limit that we send to mongot,
    // rather, it is used when adding the $limit stage to the merging pipeline in a sharded cluster.
    // This allows us to limit the documents that are returned from the shards as much as possible
    // without adding complicated rules for pipeline splitting.
    // The limit that we send to mongot is received and stored on the '_request' object above.
    long long _limit;
};
}  // namespace mongo
