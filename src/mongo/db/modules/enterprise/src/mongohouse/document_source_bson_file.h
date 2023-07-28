/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

class DocumentSourceBSONFile final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$bsonFile"_sd;
    ~DocumentSourceBSONFile() final;

    const char* getSourceName() const final;
    Value serialize(const SerializationOptions& opts = SerializationOptions{}) const final override;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
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

    static boost::intrusive_ptr<DocumentSourceBSONFile> create(
        const boost::intrusive_ptr<ExpressionContext>& pExpCtx, StringData fileName);

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    void addVariableRefs(std::set<Variables::Id>* refs) const final {}

protected:
    GetNextResult doGetNext() final;
    void doDispose() final;

private:
    DocumentSourceBSONFile(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                           StringData fileName);

    std::string _fileName;

#ifdef _WIN32
    HANDLE _file = nullptr;
    HANDLE _fileMapping = nullptr;
#else
    int _fd = -1;
#endif
    size_t _fileSize = 0;
    void* _mapped;
    off_t _offset = 0;
};

}  // namespace mongo
