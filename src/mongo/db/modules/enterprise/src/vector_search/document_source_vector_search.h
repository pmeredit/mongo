/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/executor/task_executor_cursor.h"
#include "vector_search/document_source_vector_search_gen.h"

namespace mongo {

/**
 * A class to retrieve kNN results from a mongot process.
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

    /**
     * Allows stages preserving order and metadata to move past during split. This allows
     * the stages like $_internalSearchIdLookup to stay in shards stages.
     */
    static bool canMovePastDuringSplit(const DocumentSource& ds) {
        return ds.constraints().preservesOrderAndMetadata;
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        DistributedPlanLogic logic;
        logic.shardsStage = this;
        logic.mergeSortPattern = kSortSpec;
        logic.needsSplit = false;
        logic.canMovePast = canMovePastDuringSplit;
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
};
}  // namespace mongo
