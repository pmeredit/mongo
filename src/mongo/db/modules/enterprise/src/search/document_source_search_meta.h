/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "document_source_internal_search_mongot_remote.h"
#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * The $searchMeta stage is similar to the $_internalMongotRemote stage except that it consumes
 * metadata cursors.
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

    size_t getRemoteCursorId() {
        return _remoteCursorId;
    }

    void setRemoteCursorVars(boost::optional<BSONObj> remoteCursorVars) {
        if (remoteCursorVars) {
            _remoteCursorVars = remoteCursorVars->getOwned();
        }
    }

    boost::optional<BSONObj> getRemoteCursorVars() const {
        return _remoteCursorVars;
    }

    boost::optional<executor::TaskExecutorCursor> getCursor() {
        return std::move(_cursor);
    }

protected:
    virtual Value serialize(
        const SerializationOptions& opts = SerializationOptions{}) const final override;

    executor::TaskExecutorCursor establishCursor() override;

private:
    GetNextResult getNextAfterSetup() override;

    bool _returnedAlready = false;

    // An unique id of search stage in the pipeline, currently it is hard coded to 0 because we can
    // only have one search stage and sub-pipelines are not in the same PlanExecutor.
    // We should assign unique ids when we have everything in a single PlanExecutorSBE.
    size_t _remoteCursorId{0};

    boost::optional<BSONObj> _remoteCursorVars;
};

}  // namespace mongo
