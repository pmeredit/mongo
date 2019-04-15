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

    invariant(pExpCtx->uuid);
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

            result = pExpCtx->mongoProcessInterface->lookupSingleDocument(
                pExpCtx, pExpCtx->ns, pExpCtx->uuid.get(), documentKey, boost::none);
        }
    }

    // Result must be populated here - EOF returns above.
    invariant(result);
    MutableDocument output(*result);

    // TODO (SERVER-40016): Add correct metadata handling.

    return output.freeze();
}

const char* DocumentSourceInternalSearchBetaIdLookUp::getSourceName() const {
    return "$_internalSearchBetaIdLookup";
}

}  // namespace mongo
