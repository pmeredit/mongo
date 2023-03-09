/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "backup_cursor_service.h"
#include "hot_backups/document_source_backup_file_gen.h"
#include "mongo/db/pipeline/document_source.h"
#include <fstream>

namespace mongo {

class DocumentSourceBackupFile : public DocumentSource {
public:
    static constexpr StringData kStageName = "$backupFile"_sd;

    DocumentSourceBackupFile(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                             DocumentSourceBackupFileSpec spec);

    static boost::intrusive_ptr<DocumentSourceBackupFile> create(
        const boost::intrusive_ptr<ExpressionContext>&, DocumentSourceBackupFileSpec spec);

    static boost::intrusive_ptr<DocumentSourceBackupFile> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx);

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const;

    Value serialize(SerializationOptions opts) const final override {
        MONGO_UNIMPLEMENTED;
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    const char* getSourceName() const {
        return DocumentSourceBackupFile::kStageName.rawData();
    }
    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     // This stage `uasserts` on a MongoS; the
                                     // `HostTypeRequirement` field has no effect.
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kNotAllowed,
                                     UnionRequirement::kNotAllowed);

        constraints.isIndependentOfAnyCollection = true;
        constraints.requiresInputDocSource = false;
        return constraints;
    }

    void addVariableRefs(std::set<Variables::Id>* refs) const final {}

protected:
    DocumentSource::GetNextResult doGetNext() override;
    void doDispose() override;

private:
    // Ensure the backup session has not been closed while the backup file was being read.
    void checkBackupSessionStillValid();

    DocumentSourceBackupFileSpec _backupFileSpec;
    std::fstream _src;
    bool _eof = false;
    std::streamoff _offset;

    // If there a length to read specified, we will store that value in '_remainingLengthToRead'. If
    // there is no length specified, we will read until the end of the file.
    std::streamsize _remainingLengthToRead;
};

}  // namespace mongo
