/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

class DocumentSourceBSONFile : public DocumentSource {
public:
    DocumentSourceBSONFile(const boost::intrusive_ptr<ExpressionContext>& pCtx,
                           const char* fileName);
    virtual ~DocumentSourceBSONFile();

    GetNextResult getNext() override;
    const char* getSourceName() const override;
    Value serialize(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const override;

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kAllowed);

        constraints.requiresInputDocSource = false;
        return constraints;
    }

    static boost::intrusive_ptr<DocumentSourceBSONFile> create(
        const boost::intrusive_ptr<ExpressionContext>& pCtx, const char* fileName);

    void reattachToOperationContext(OperationContext* opCtx) {
        isDetachedFromOpCtx = false;
    }

    void detachFromOperationContext() {
        isDetachedFromOpCtx = true;
    }

    boost::intrusive_ptr<DocumentSource> optimize() override {
        isOptimized = true;
        return this;
    }

    boost::optional<MergingLogic> mergingLogic() override {
        return boost::none;
    }

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

    bool isDisposed = false;
    bool isDetachedFromOpCtx = false;
    bool isOptimized = false;
    bool isExpCtxInjected = false;

protected:
    void doDispose() override;
};

}  // namespace mongo
