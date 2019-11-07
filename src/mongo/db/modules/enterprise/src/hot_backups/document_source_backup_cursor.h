/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/repl/read_concern_args.h"
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
        static std::unique_ptr<LiteParsed> parse(const AggregationRequest& request,
                                                 const BSONElement& spec) {
            return std::make_unique<LiteParsed>();
        }

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos) const final {
            return {Privilege(ResourcePattern::forClusterResource(), ActionSet{ActionType::fsync})};
        }

        bool isInitialSource() const final {
            return true;
        }

        bool allowedToPassthroughFromMongos() const final {
            return false;
        }

        void assertSupportsReadConcern(const repl::ReadConcernArgs& readConcern) const {
            onlyReadConcernLocalSupported(DocumentSourceBackupCursor::kStageName, readConcern);
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

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     // This stage `uasserts` on a MongoS; the
                                     // `HostTypeRequirement` field has no effect.
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kNotAllowed);

        constraints.isIndependentOfAnyCollection = true;
        constraints.requiresInputDocSource = false;
        return constraints;
    }

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

private:
    DocumentSourceBackupCursor(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                               bool incrementalBackup,
                               boost::optional<std::string> thisBackupName,
                               boost::optional<std::string> srcBackupName);

    GetNextResult doGetNext() final;

    BackupCursorState _backupCursorState;
};

}  // namespace mongo
