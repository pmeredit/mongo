/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * The $searchMeta stage is an alias for [$_internalSearchMongotRemote,
 * $replaceWith, $unionWith, $limit] to only return the meta results from a $search query.
 */
class DocumentSourceSearchMeta final : public DocumentSourceInternalSearchMongotRemote {
public:
    static constexpr StringData kStageName = "$searchMeta"_sd;

    static std::list<boost::intrusive_ptr<DocumentSource>> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx);

    // Same construction API as the parent class.
    using DocumentSourceInternalSearchMongotRemote::DocumentSourceInternalSearchMongotRemote;

    const char* getSourceName() const {
        return kStageName.rawData();
    }

    /**
     * This is the first stage in the pipeline, but we need to gather responses from all shards in
     * order to set $$SEARCH_META appropriately.
     */
    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        DistributedPlanLogic logic;
        logic.shardsStage = this;
        tassert(6448011, "Expected merging pipeline to be set already", _mergingPipeline);
        logic.mergingStages = _mergingPipeline->getSources();
        return logic;
    }

protected:
    virtual Value serialize(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const override;

    Value serialize(SerializationOptions opts) const final override {
        MONGO_UNIMPLEMENTED;
    }

    executor::TaskExecutorCursor establishCursor() override;

private:
    GetNextResult getNextAfterSetup() override;

    bool _returnedAlready = false;
};

}  // namespace mongo
