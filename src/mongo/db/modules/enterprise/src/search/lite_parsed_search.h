/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/db/pipeline/lite_parsed_pipeline.h"

namespace mongo {
/**
 * A 'LiteParsed' representation of either a $search or $searchMeta stage.
 */
class LiteParsedSearchStage final : public LiteParsedDocumentSource {
public:
    static std::unique_ptr<LiteParsedSearchStage> parse(const NamespaceString& nss,
                                                        const BSONElement& spec) {
        return std::make_unique<LiteParsedSearchStage>(spec.fieldName(), nss);
    }

    stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const override {
        // There are no foreign namespaces.
        return stdx::unordered_set<NamespaceString>{};
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
        return onlyReadConcernLocalSupported(getParseTimeName(), level, isImplicitDefault);
    }

    void assertSupportsMultiDocumentTransaction() const {
        transactionNotSupported(getParseTimeName());
    }

    explicit LiteParsedSearchStage(std::string parseTimeName, NamespaceString nss)
        : LiteParsedDocumentSource(std::move(parseTimeName)), _nss(std::move(nss)) {}

private:
    const NamespaceString _nss;
};
}  // namespace mongo
