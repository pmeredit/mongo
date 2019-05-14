/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/optional.hpp>

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * Queries local collection for _id equality matches. Intended for use with
 * $_internalSearchBetaMongotRemote (see $searchBeta) as part of the SearchBeta project.
 *
 * Input documents will be ignored and skipped if they do not have a value at field "_id".
 * Input documents will be ignored and skipped if no document with key specified at "_id"
 * is locally-stored.
 *
 * Work slated and not handled yet:
 * - TODO Ensure this handles sharded sort correctly (SERVER-40015)
 * - TODO Handle searchSnippet metadata (SERVER-40555)
 */
class DocumentSourceInternalSearchBetaIdLookUp final : public DocumentSource {
public:
    /**
     * Creates an $_internalSearchBetaIdLookup stage. "elem" must be an empty object.
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    DocumentSourceInternalSearchBetaIdLookUp(
        const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    const char* getSourceName() const final;

    GetNextResult getNext() final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kNone,
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     ChangeStreamRequirement::kBlacklist);

        return constraints;
    }

    /**
     * Serialize this stage - return is of the form { $_internalSearchBetaIdLookup: {} }
     */
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const;

    /**
     * This stage must be run on each shard and will cause the pipeline to split. mongos
     * will merge by searchScore.
     */
    boost::optional<MergingLogic> mergingLogic() final {
        return boost::none;  // TODO handle sharded sort correctly (SERVER-40015)
    }
};

}  // namespace mongo
