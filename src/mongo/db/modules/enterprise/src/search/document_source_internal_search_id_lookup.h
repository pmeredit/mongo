/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/optional.hpp>

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * Queries local collection for _id equality matches. Intended for use with
 * $_internalSearchMongotRemote (see $search) as part of the Search project.
 *
 * Input documents will be ignored and skipped if they do not have a value at field "_id".
 * Input documents will be ignored and skipped if no document with key specified at "_id"
 * is locally-stored.
 */
class DocumentSourceInternalSearchIdLookUp final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$_internalSearchIdLookup"_sd;
    /**
     * Creates an $_internalSearchIdLookup stage. "elem" must be an empty object.
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    DocumentSourceInternalSearchIdLookUp(const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    const char* getSourceName() const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kNone,
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kAllowed,
                                     UnionRequirement::kAllowed,
                                     ChangeStreamRequirement::kDenylist);
        // Set to true to allow this to be run on the shards before the search implicit sort.
        constraints.preservesOrderAndMetadata = true;

        return constraints;
    }

    /**
     * Serialize this stage - return is of the form { $_internalSearchIdLookup: {} }
     */
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const;

    /**
     * This stage must be run on each shard.
     */
    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        DistributedPlanLogic logic;

        logic.shardsStage = this;

        return logic;
    }

private:
    DocumentSource::GetNextResult doGetNext() final;
};

}  // namespace mongo
