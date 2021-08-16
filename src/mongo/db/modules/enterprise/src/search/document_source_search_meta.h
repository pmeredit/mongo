/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/lite_parsed_pipeline.h"

namespace mongo {

/**
 * The $searchMeta stage is an alias for [$_internalSearchMongotRemote,
 * $replaceWith, $unionWith, $limit] to only return the meta results from a $search query.
 */
class DocumentSourceSearchMeta final {
public:
    static constexpr StringData kStageName = "$searchMeta"_sd;

    static std::list<boost::intrusive_ptr<DocumentSource>> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const NamespaceString& nss,
                                                 const BSONElement& spec) {
            return std::make_unique<LiteParsed>(
                spec.fieldName(), NamespaceString::makeCollectionlessAggregateNSS(nss.db()));
        }

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const override {
            // Make sure the collectionless namespace is included.
            stdx::unordered_set<NamespaceString> namespaces{};
            namespaces.insert(_nss);
            return namespaces;
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
    // It is illegal to construct a DocumentSourceSearchMeta directly, use createFromBson()
    // instead.
    DocumentSourceSearchMeta() = default;
};

}  // namespace mongo
