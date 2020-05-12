/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

class DocumentSourceStdin final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$stdin"_sd;
    ~DocumentSourceStdin() final {}

    const char* getSourceName() const final;
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kAllowed,
                                     LookupRequirement::kNotAllowed,
                                     UnionRequirement::kNotAllowed);

        constraints.requiresInputDocSource = false;
        return constraints;
    }

    static boost::intrusive_ptr<DocumentSourceStdin> create(
        const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

private:
    DocumentSourceStdin(const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    DocumentSource::GetNextResult doGetNext() final;
};

}  // namespace mongo
