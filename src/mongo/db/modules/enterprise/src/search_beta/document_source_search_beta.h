/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/pipeline/document_source.h"

namespace mongo {

/**
 * The $searchBeta stage is an alias for [$_internalSearchBetaMongotRemote,
 * $_internalSearchBetaIdLookup] stages associated with an $searchBeta query.
 */
class DocumentSourceSearchBeta final {
public:
    static constexpr StringData kStageName = "$searchBeta"_sd;

    static std::list<boost::intrusive_ptr<DocumentSource>> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const AggregationRequest& request,
                                                 const BSONElement& spec) {
            return std::make_unique<LiteParsed>(request.getNamespaceString());
        }

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const override {
            // There are no foreign collections.
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos) const override {
            return {Privilege(ResourcePattern::forExactNamespace(_nss), ActionType::find)};
        }

        bool isInitialSource() const final {
            return true;
        }

        void assertSupportsReadConcern(const repl::ReadConcernArgs& readConcern) const {
            onlyReadConcernLocalSupported(kStageName, readConcern);
        }

        void assertSupportsMultiDocumentTransaction() const {
            transactionNotSupported(kStageName);
        }

        explicit LiteParsed(NamespaceString nss) : _nss(std::move(nss)) {}

    private:
        const NamespaceString _nss;
    };

    const char* getSourceName() const;

private:
    // It is illegal to construct a DocumentSourceSearchBeta directly, use createFromBson()
    // instead.
    DocumentSourceSearchBeta() = default;
};

}  // namespace mongo
