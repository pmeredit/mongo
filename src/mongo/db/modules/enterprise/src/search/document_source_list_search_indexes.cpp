/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "document_source_list_search_indexes.h"
#include "search/search_index_commands.h"

namespace mongo {

REGISTER_DOCUMENT_SOURCE(listSearchIndexes,
                         DocumentSourceListSearchIndexes::LiteParsed::parse,
                         DocumentSourceListSearchIndexes::createFromBson,
                         AllowedWithApiStrict::kNeverInVersion1)

void DocumentSourceListSearchIndexes::validateListSearchIndexesSpec(
    const DocumentSourceListSearchIndexesSpec* spec) {
    uassert(ErrorCodes::InvalidOptions,
            "Cannot set both 'name' and 'id' for $listSearchIndexes.",
            !(spec->getId() && spec->getName()));
};

boost::intrusive_ptr<DocumentSource> DocumentSourceListSearchIndexes::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "The $listSearchIndexes stage specification must be an object. Found: "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object);
    auto spec = DocumentSourceListSearchIndexesSpec::parse(IDLParserContext(kStageName),
                                                           elem.embeddedObject());

    return new DocumentSourceListSearchIndexes(pExpCtx, elem.Obj());
}

Value DocumentSourceListSearchIndexes::serialize(SerializationOptions opts) const {
    BSONObjBuilder bob;
    auto spec = DocumentSourceListSearchIndexesSpec::parse(IDLParserContext(kStageName), _cmdObj);
    spec.serialize(&bob, opts);
    return Value(Document{{kStageName, bob.done()}});
}

StageConstraints DocumentSourceListSearchIndexes::constraints(
    Pipeline::SplitState pipeState) const {
    StageConstraints constraints(StreamType::kStreaming,
                                 PositionRequirement::kFirst,
                                 HostTypeRequirement::kNone,
                                 DiskUseRequirement::kNoDiskUse,
                                 FacetRequirement::kNotAllowed,
                                 TransactionRequirement::kNotAllowed,
                                 LookupRequirement::kAllowed,
                                 UnionRequirement::kAllowed,
                                 ChangeStreamRequirement::kDenylist);
    constraints.requiresInputDocSource = false;
    return constraints;
}

DocumentSource::GetNextResult DocumentSourceListSearchIndexes::doGetNext() {
    /**
     * The user command field of the 'manageSearchIndex' command should be the stage issued by the
     * user. The 'id' field and 'name' field are optional, so possible user commands can be:
     * $listSearchIndexes: {}
     * $listSearchIndexes: { id: "<index id>" }
     * $listSearchIndexes: { name: "<index name>"}
     */
    if (_searchIndexes.empty()) {
        BSONObjBuilder bob;
        bob.append(kStageName, _cmdObj);
        // Sends a manageSearchIndex command and returns a cursor with index information.
        BSONObj manageSearchIndexResponse =
            runSearchIndexCommand(pExpCtx->opCtx, pExpCtx->ns, bob.done());

        /**
         * 'mangeSearchIndex' returns a cursor with the following fields:
         * cursor: {
         *   id: Long("0"),
         *   ns: "<database name>.
         *   firstBatch: [ // There will only ever be one batch.
         *       {<document>},
         *       {<document>} ],
         * }
         * We need to return the documents in the 'firstBatch' field.
         */
        auto cursor = manageSearchIndexResponse.getField(kCursorFieldName);
        tassert(7486302,
                "internal command manageSearchIndex should return a 'cursor' field with an object.",
                !cursor.eoo() && cursor.type() == BSONType::Object);

        cursor = cursor.Obj().getField(kFirstBatchFieldName);
        tassert(7486303,
                "internal command manageSearchIndex should return an array of documents in the "
                "'firstBatch' field",
                !cursor.eoo() && cursor.type() == BSONType::Array);
        _searchIndexes = cursor.Array();
        _searchIndexesIter = _searchIndexes.cbegin();
    }

    if (_searchIndexesIter != _searchIndexes.cend()) {
        Document doc{std::move(_searchIndexesIter->Obj())};
        ++_searchIndexesIter;
        return doc;
    }

    return GetNextResult::makeEOF();
}

}  // namespace mongo
