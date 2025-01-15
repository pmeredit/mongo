/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/storage/backup_cursor_state.h"

namespace mongo {

/**
 * Represents the `$backupCursorExtend` aggregation stage. The DocumentSource will return filenames
 * of the extra journal logs in the running `dbpath`.
 */
class DocumentSourceBackupCursorExtend final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$backupCursorExtend"_sd;

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const NamespaceString& nss,
                                                 const BSONElement& spec) {
            return std::make_unique<LiteParsed>(spec.fieldName(), nss.tenantId());
        }

        explicit LiteParsed(std::string parseTimeName, const boost::optional<TenantId>& tenantId)
            : LiteParsedDocumentSource(std::move(parseTimeName)),
              _privileges({Privilege(ResourcePattern::forClusterResource(tenantId),
                                     ActionSet{ActionType::fsync})}) {}

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos,
                                           bool bypassDocumentValidation) const final {
            return _privileges;
        }

        bool isInitialSource() const final {
            return true;
        }

        ReadConcernSupportResult supportsReadConcern(repl::ReadConcernLevel level,
                                                     bool isImplicitDefault) const override {
            return onlyReadConcernLocalSupported(kStageName, level, isImplicitDefault);
        }

        void assertSupportsMultiDocumentTransaction() const override {
            transactionNotSupported(DocumentSourceBackupCursorExtend::kStageName);
        }

    private:
        const PrivilegeVector _privileges;
    };

    const char* getSourceName() const final {
        return DocumentSourceBackupCursorExtend::kStageName.rawData();
    }

    static const Id& id;

    Id getId() const override {
        return id;
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() override {
        return boost::none;
    }

    Value serialize(const SerializationOptions& opts = SerializationOptions{}) const final;

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
    DocumentSourceBackupCursorExtend(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                                     const UUID& backupId,
                                     const Timestamp& extendTo);

    GetNextResult doGetNext() final;

    const UUID _backupId;
    const Timestamp _extendTo;
    BackupCursorExtendState _backupCursorExtendState;
};

}  // namespace mongo
