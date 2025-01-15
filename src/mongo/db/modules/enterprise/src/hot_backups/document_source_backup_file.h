/**
 * Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <fstream>

#include "backup_cursor_service.h"
#include "hot_backups/document_source_backup_file_gen.h"

#include "mongo/db/pipeline/document_source.h"

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

    Value serialize(const SerializationOptions& opts = SerializationOptions{}) const final;

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    const char* getSourceName() const override {
        return DocumentSourceBackupFile::kStageName.rawData();
    }

    static const Id& id;

    Id getId() const override {
        return id;
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
    enum class ExecState { kUninitialized, kActive, kEof };
    static constexpr bool isEof(ExecState s) {
        return s == ExecState::kEof;
    }

    // Returns true iff the requested file is returned by the named backup ID (both given by
    // _backupFileSpec) AND that this backup ID is currently active.
    bool backupSessionIsValid() const;

    // Before we start executing, make sure this is a sensible request. We check this when execution
    // starts to ensure that this DocumentSource can be parsed and inspected without any particular
    // state needing to be set up.
    void prepareForExecution();

    // Ensure the backup session has not been closed while the backup file was being read.
    void checkBackupSessionStillValid() const;

    DocumentSourceBackupFileSpec _backupFileSpec;
    std::fstream _src;
    ExecState _execState = ExecState::kUninitialized;
    std::streamoff _offset;

    // If there a length to read specified, we will store that value in '_remainingLengthToRead'. If
    // there is no length specified, we will read until the end of the file.
    std::streamsize _remainingLengthToRead;
};

}  // namespace mongo
