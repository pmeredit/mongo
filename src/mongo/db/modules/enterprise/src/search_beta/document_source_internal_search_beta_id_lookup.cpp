/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_internal_search_beta_id_lookup.h"

#include "mongo/db/pipeline/document.h"

namespace mongo {

using boost::intrusive_ptr;

REGISTER_DOCUMENT_SOURCE(_internalSearchBetaIdLookup,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceInternalSearchBetaIdLookUp::createFromBson);

DocumentSourceInternalSearchBetaIdLookUp::DocumentSourceInternalSearchBetaIdLookUp(
    const intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSource(pExpCtx) {}

intrusive_ptr<DocumentSource> DocumentSourceInternalSearchBetaIdLookUp::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(31016,
            str::stream() << "$_internalSearchBetaIdLookup value must be an empty object. Found: "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object && elem.embeddedObject().isEmpty());

    return new DocumentSourceInternalSearchBetaIdLookUp(pExpCtx);
}

Value DocumentSourceInternalSearchBetaIdLookUp::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << Document()));
}

DocumentSource::GetNextResult DocumentSourceInternalSearchBetaIdLookUp::getNext() {
    pExpCtx->checkForInterrupt();

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
                    "Collection must have a UUID to use $_internalSearchBetaIdLookup.",
                    pExpCtx->uuid.has_value());

            // Find the document by performing a local read.
            MongoProcessInterface::MakePipelineOptions pipelineOpts;
            pipelineOpts.attachCursorSource = false;
            auto pipeline = pExpCtx->mongoProcessInterface->makePipeline(
                {BSON("$match" << documentKey)}, pExpCtx, pipelineOpts);

            pipeline = pExpCtx->mongoProcessInterface->attachCursorSourceToPipelineForLocalRead(
                pExpCtx, pipeline.release());

            result = pipeline->getNext();
            if (auto next = pipeline->getNext()) {
                uasserted(ErrorCodes::TooManyMatchingDocuments,
                          str::stream() << "found more than one document with document key "
                                        << documentKey.toString()
                                        << ": ["
                                        << result->toString()
                                        << ", "
                                        << next->toString()
                                        << "]");
            }
        }
    }

    // Result must be populated here - EOF returns above.
    invariant(result);
    MutableDocument output(*result);

    // Transfer searchScore metadata from inputDoc to the result.
    output.copyMetaDataFrom(inputDoc);

    if (pExpCtx->needsMerge) {
        output.setSortKeyMetaField(BSON("" << inputDoc.getSearchScore()));
    }

    return output.freeze();
}

const char* DocumentSourceInternalSearchBetaIdLookUp::getSourceName() const {
    return "$_internalSearchBetaIdLookup";
}

}  // namespace mongo
