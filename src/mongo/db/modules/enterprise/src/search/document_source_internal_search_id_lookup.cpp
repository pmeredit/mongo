/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_internal_search_id_lookup.h"

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"

namespace mongo {

using boost::intrusive_ptr;

REGISTER_DOCUMENT_SOURCE(_internalSearchIdLookup,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceInternalSearchIdLookUp::createFromBson,
                         LiteParsedDocumentSource::AllowedWithApiStrict::kInternal);

DocumentSourceInternalSearchIdLookUp::DocumentSourceInternalSearchIdLookUp(
    const intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSource(kStageName, pExpCtx) {}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceInternalSearchIdLookUp::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(31016,
            str::stream() << "$_internalSearchIdLookup value must be an empty object. Found: "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object && elem.embeddedObject().isEmpty());

    std::list<boost::intrusive_ptr<DocumentSource>> list = {
        new DocumentSourceInternalSearchIdLookUp(pExpCtx)};

    if (auto shardFilterer = pExpCtx->mongoProcessInterface->getShardFilterer(pExpCtx)) {
        list.push_back(new DocumentSourceInternalShardFilter(pExpCtx, std::move(shardFilterer)));
    }

    return list;
}

Value DocumentSourceInternalSearchIdLookUp::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << Document()));
}

DocumentSource::GetNextResult DocumentSourceInternalSearchIdLookUp::doGetNext() {
    boost::optional<Document> result;
    Document inputDoc;

    while (!result) {
        auto nextInput = pSource->getNext();
        if (!nextInput.isAdvanced()) {
            return nextInput;
        }

        inputDoc = nextInput.releaseDocument();
        auto documentId = inputDoc["_id"];

        if (!documentId.missing()) {
            auto documentKey = Document({{"_id", documentId}});

            uassert(31052,
                    "Collection must have a UUID to use $_internalSearchIdLookup.",
                    pExpCtx->uuid.has_value());

            // Find the document by performing a local read.
            MakePipelineOptions pipelineOpts;
            pipelineOpts.attachCursorSource = false;
            auto pipeline =
                Pipeline::makePipeline({BSON("$match" << documentKey)}, pExpCtx, pipelineOpts);

            pipeline = pExpCtx->mongoProcessInterface->attachCursorSourceToPipelineForLocalRead(
                pipeline.release());

            result = pipeline->getNext();
            if (auto next = pipeline->getNext()) {
                uasserted(ErrorCodes::TooManyMatchingDocuments,
                          str::stream() << "found more than one document with document key "
                                        << documentKey.toString() << ": [" << result->toString()
                                        << ", " << next->toString() << "]");
            }
        }
    }

    // Result must be populated here - EOF returns above.
    invariant(result);
    MutableDocument output(*result);

    // Transfer searchScore metadata from inputDoc to the result.
    output.copyMetaDataFrom(inputDoc);

    if (pExpCtx->needsMerge) {
        auto isSingleElementKey = true;
        output.metadata().setSortKey(Value{inputDoc.metadata().getSearchScore()},
                                     isSingleElementKey);
    }

    return output.freeze();
}

const char* DocumentSourceInternalSearchIdLookUp::getSourceName() const {
    return kStageName.rawData();
}

}  // namespace mongo
