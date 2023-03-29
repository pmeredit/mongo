/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "document_source_internal_search_id_lookup.h"

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source_internal_shard_filter.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "search/document_source_internal_search_mongot_remote_gen.h"

namespace mongo {

using boost::intrusive_ptr;

REGISTER_DOCUMENT_SOURCE(_internalSearchIdLookup,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceInternalSearchIdLookUp::createFromBson,
                         AllowedWithApiStrict::kInternal);

DocumentSourceInternalSearchIdLookUp::DocumentSourceInternalSearchIdLookUp(
    const intrusive_ptr<ExpressionContext>& pExpCtx)
    : DocumentSource(kStageName, pExpCtx) {}

DocumentSourceInternalSearchIdLookUp::DocumentSourceInternalSearchIdLookUp(
    const intrusive_ptr<ExpressionContext>& pExpCtx, long long limit)
    : DocumentSource(kStageName, pExpCtx), _limit(limit) {}

intrusive_ptr<DocumentSource> DocumentSourceInternalSearchIdLookUp::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(
        31016,
        str::stream() << "$_internalSearchIdLookup value must be an empty object or just have "
                         "one field called 'limit'. Found: "
                      << typeName(elem.type()),
        elem.type() == BSONType::Object &&
            (elem.embeddedObject().isEmpty() ||
             ((elem.embeddedObject().nFields() == 1) &&
              elem.embeddedObject().hasField(InternalSearchMongotRemoteSpec::kLimitFieldName))));
    auto specObj = elem.embeddedObject();
    if (specObj.hasField(InternalSearchMongotRemoteSpec::kLimitFieldName)) {
        auto limitElem = specObj.getField(InternalSearchMongotRemoteSpec::kLimitFieldName);
        uassert(6770001, "Limit must be a long", limitElem.type() == BSONType::NumberLong);
        return new DocumentSourceInternalSearchIdLookUp(pExpCtx, limitElem.Long());
    }
    return new DocumentSourceInternalSearchIdLookUp(pExpCtx);
}

Value DocumentSourceInternalSearchIdLookUp::serialize(SerializationOptions opts) const {
    auto internalDoc = _limit == 0 ? Document()
                                   : DOC(InternalSearchMongotRemoteSpec::kLimitFieldName
                                         << opts.serializeLiteralValue(Value((long long)_limit)));
    return Value(DOC(getSourceName() << internalDoc));
}

DocumentSource::GetNextResult DocumentSourceInternalSearchIdLookUp::doGetNext() {
    boost::optional<Document> result;
    Document inputDoc;
    if (_limit != 0 && _docsReturned >= _limit) {
        return DocumentSource::GetNextResult::makeEOF();
    }
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
    ++_docsReturned;
    return output.freeze();
}

const char* DocumentSourceInternalSearchIdLookUp::getSourceName() const {
    return kStageName.rawData();
}

Pipeline::SourceContainer::iterator DocumentSourceInternalSearchIdLookUp::doOptimizeAt(
    Pipeline::SourceContainer::iterator itr, Pipeline::SourceContainer* container) {
    for (auto optItr = std::next(itr); optItr != container->end(); ++optItr) {
        auto limitStage = dynamic_cast<DocumentSourceLimit*>(optItr->get());
        if (limitStage) {
            _limit = limitStage->getLimit();
            break;
        }
        if (!optItr->get()->constraints().canSwapWithSkippingOrLimitingStage) {
            break;
        }
    }
    return std::next(itr);
}

}  // namespace mongo
