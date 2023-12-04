/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once
#include <queue>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/pipeline/document_source.h"
#include "search/document_source_list_search_indexes_gen.h"

namespace mongo {
class DocumentSourceListSearchIndexesSpec;

class DocumentSourceListSearchIndexes final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$listSearchIndexes"_sd;
    static constexpr StringData kCursorFieldName = "cursor"_sd;
    static constexpr StringData kFirstBatchFieldName = "firstBatch"_sd;

    static void validateListSearchIndexesSpec(const DocumentSourceListSearchIndexesSpec* spec);
    /**
     * A 'LiteParsed' representation of the $listSearchIndexes stage.
     */
    class LiteParsedListSearchIndexes final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsedListSearchIndexes> parse(const NamespaceString& nss,
                                                                  const BSONElement& spec) {
            return std::make_unique<LiteParsedListSearchIndexes>(spec.fieldName(), nss);
        }

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const override {
            // There are no foreign namespaces.
            return stdx::unordered_set<NamespaceString>{};
        }

        PrivilegeVector requiredPrivileges(bool isMongos,
                                           bool bypassDocumentValidation) const override {
            return {Privilege(ResourcePattern::forDatabaseName(_nss.dbName()),
                              ActionType::listSearchIndexes)};
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

        explicit LiteParsedListSearchIndexes(std::string parseTimeName, NamespaceString nss)
            : LiteParsedDocumentSource(std::move(parseTimeName)), _nss(std::move(nss)) {}

    private:
        const NamespaceString _nss;
    };

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    DocumentSourceListSearchIndexes(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                                    BSONObj cmdObj)
        : DocumentSource(kStageName, pExpCtx), _cmdObj(cmdObj.getOwned()) {}

    const char* getSourceName() const override {
        return kStageName.rawData();
    }

    virtual Value serialize(
        const SerializationOptions& opts = SerializationOptions{}) const final override;

    void addVariableRefs(std::set<Variables::Id>* refs) const final {}

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    StageConstraints constraints(Pipeline::SplitState pipeState) const final;

private:
    GetNextResult doGetNext() final;
    BSONObj _cmdObj;
    std::queue<BSONObj> _searchIndexes;
    bool _eof = false;
};

}  // namespace mongo
