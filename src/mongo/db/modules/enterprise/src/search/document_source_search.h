/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * The $search stage expands to multiple internal stages when parsed, namely
 * $_internalSearchMongotRemote and $_internalSearchIdLookup. $setVariableFromSubPipeline may also
 * be added to handle $$SEARCH_META assignment.
 *
 * We only ever make a DocumentSourceSearch for a pipeline to store it in the view catalog.
 * Desugaring must be done every time the view is called.
 */
class DocumentSourceSearch final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$search"_sd;
    static constexpr StringData kReturnStoredSourceArg = "returnStoredSource"_sd;
    static constexpr StringData kProtocolStoredFieldsName = "storedSource"_sd;

    static std::list<boost::intrusive_ptr<DocumentSource>> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const NamespaceString& nss,
                                                 const BSONElement& spec) {
            return std::make_unique<LiteParsed>(spec.fieldName(), nss);
        }

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const override {
            // There are no foreign collections.
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos,
                                           bool bypassDocumentValidation) const override {
            return {Privilege(ResourcePattern::forExactNamespace(_nss), ActionType::find)};
        }

        bool isInitialSource() const final {
            return true;
        }

        ReadConcernSupportResult supportsReadConcern(repl::ReadConcernLevel level,
                                                     bool isImplicitDefault) const {
            return onlyReadConcernLocalSupported(kStageName, level, isImplicitDefault);
        }

        void assertSupportsMultiDocumentTransaction() const {
            transactionNotSupported(kStageName);
        }

        explicit LiteParsed(std::string parseTimeName, NamespaceString nss)
            : LiteParsedDocumentSource(std::move(parseTimeName)), _nss(std::move(nss)) {}

    private:
        const NamespaceString _nss;
    };

    const char* getSourceName() const;

    StageConstraints constraints(Pipeline::SplitState pipeState) const override;

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        // This stage should never be used in a distributed plan.
        MONGO_UNREACHABLE_TASSERT(6253715);
    }


private:
    // Pipelines usually should not construct a DocumentSourceSearch directly, use createFromBson()
    // instead.
    DocumentSourceSearch() = default;
    DocumentSourceSearch(BSONObj spec, const boost::intrusive_ptr<ExpressionContext> expCtx)
        : DocumentSource(kStageName, expCtx), _userObj(std::move(spec)) {}
    virtual Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const;

    GetNextResult doGetNext() {
        // We should never execute a DocumentSourceSearch.
        MONGO_UNREACHABLE_TASSERT(6253716);
    }

    // The original stage specification that was the value for the "$search" field of the owning
    // object.
    BSONObj _userObj;
};

}  // namespace mongo
