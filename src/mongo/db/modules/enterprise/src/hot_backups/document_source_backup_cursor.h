/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/storage/backup_cursor_hooks.h"
#include "mongo/db/storage/backup_cursor_state.h"

namespace mongo {

/**
 * Represents the `$backupCursor` aggregation stage. The lifetime of this object maps to storage
 * engine calls on `beginNonBlockingBackup` and `endNonBlockingBackup`. The DocumentSource will
 * return filenames in the running `dbpath` that an application can copy and optionally some
 * metadata information.
 */
class DocumentSourceBackupCursor final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$backupCursor"_sd;

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const NamespaceString& nss,
                                                 const BSONElement& spec) {
            return std::make_unique<LiteParsed>(spec.fieldName());
        }
        explicit LiteParsed(std::string parseTimeName)
            : LiteParsedDocumentSource(std::move(parseTimeName)) {}

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos,
                                           bool bypassDocumentValidation) const final {
            return {Privilege(ResourcePattern::forClusterResource(), ActionSet{ActionType::fsync})};
        }

        bool isInitialSource() const final {
            return true;
        }

        bool allowedToPassthroughFromMongos() const final {
            return false;
        }

        ReadConcernSupportResult supportsReadConcern(repl::ReadConcernLevel level,
                                                     bool isImplicitDefault) const {
            return onlyReadConcernLocalSupported(kStageName, level, isImplicitDefault);
        }

        void assertSupportsMultiDocumentTransaction() const {
            transactionNotSupported(DocumentSourceBackupCursor::kStageName);
        }
    };

    virtual ~DocumentSourceBackupCursor();

    const char* getSourceName() const final {
        return DocumentSourceBackupCursor::kStageName.rawData();
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() {
        return boost::none;
    }

    Value serialize(SerializationOptions opts = SerializationOptions()) const final override;

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

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

private:
    DocumentSourceBackupCursor(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                               const StorageEngine::BackupOptions& options);

    /**
     * The first call to doGetNext() will return the metadata about the backup. Subsequent calls
     * return documents with the following formats.
     *
     * 1. Non-incremental (i.e. full) backups return one document per file with the format:
     *
     *    {
     *        filename: String,
     *        fileSize: Number
     *    }
     *
     * 2. The first full backup used as the basis for future incremental backups returns one
     *    document per file with the format:
     *
     *    {
     *        filename: String,
     *        fileSize: Number,
     *        offset: 0,
     *        length: fileSize
     *    }
     *
     * 3. Incremental backups return one document per unchanged file with the format:
     *
     *    {
     *        filename: String,
     *        fileSize: Number
     *    }
     *
     * 4. Incremental backups return multiple documents (one document per block, where each block
     *    has a maximum size of 'options.blockSizeMB') per changed file with the format:
     *
     *    {
     *        filename: String,
     *        fileSize: Number,
     *        offset: Number,
     *        length: Number
     *    }
     *
     * TODO (SERVER-47939): Improve/consolidate the format of documents returned by doGetNext().
     */
    GetNextResult doGetNext() final;

    BackupCursorState _backupCursorState;
    std::deque<BackupBlock> _backupBlocks;
    const std::size_t _kBatchSize;
};

}  // namespace mongo
