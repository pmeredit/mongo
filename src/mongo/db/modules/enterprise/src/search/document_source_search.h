/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * The $search stage is an alias for [$_internalSearchMongotRemote,
 * $_internalSearchIdLookup] stages associated with an $search query.
 */
class DocumentSourceSearch final {
public:
    static constexpr StringData kStageName = "$search"_sd;
    static constexpr StringData kReturnStoredFieldsArg = "returnStoredFields"_sd;
    static constexpr StringData kProtocolStoredFieldsName = "stored"_sd;

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

private:
    // It is illegal to construct a DocumentSourceSearch directly, use createFromBson()
    // instead.
    DocumentSourceSearch() = default;
};

}  // namespace mongo
