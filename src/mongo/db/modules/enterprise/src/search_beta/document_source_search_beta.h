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
    static std::list<boost::intrusive_ptr<DocumentSource>> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const AggregationRequest& request,
                                                 const BSONElement& spec) {
            return stdx::make_unique<LiteParsed>(request.getNamespaceString());
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
            uassert(ErrorCodes::InvalidOptions,
                    str::stream() << "Aggregation stage $searchBeta"
                                  << " requires read concern local but found "
                                  << readConcern.toString(),
                    readConcern.getLevel() == repl::ReadConcernLevel::kLocalReadConcern);
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
