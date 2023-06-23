/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "vector_search/document_source_vector_search.h"

#include "search/lite_parsed_search.h"
#include "search/search_task_executors.h"

namespace mongo {

using boost::intrusive_ptr;
using executor::RemoteCommandRequest;
using executor::TaskExecutorCursor;

REGISTER_DOCUMENT_SOURCE_WITH_FEATURE_FLAG(vectorSearch,
                                           LiteParsedSearchStage::parse,
                                           DocumentSourceVectorSearch::createFromBson,
                                           AllowedWithApiStrict::kNeverInVersion1,
                                           feature_flags::gFeatureFlagVectorSearchPublicPreview);

Value DocumentSourceVectorSearch::serialize(SerializationOptions opts) const {
    // TODO SERVER-78280 Serialize the parameters individually.
    uasserted(ErrorCodes::NotImplemented, "DocumentSourceVectorSearch::serialize()");
    return Value(Document{{kStageName, opts.serializeLiteral(_request)}});
}

DocumentSource::GetNextResult DocumentSourceVectorSearch::doGetNext() {
    // TODO SERVER-78280 Send request to mongot.
    uasserted(ErrorCodes::NotImplemented, "DocumentSourceVectorSearch::doGetNext()");
    return DocumentSource::GetNextResult::makeEOF();
}

intrusive_ptr<DocumentSource> DocumentSourceVectorSearch::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << kStageName
                          << " value must be an object. Found: " << typeName(elem.type()),
            elem.type() == BSONType::Object);
    auto serviceContext = expCtx->opCtx->getServiceContext();
    return new DocumentSourceVectorSearch(
        elem.embeddedObject(), expCtx, executor::getMongotTaskExecutor(serviceContext));
}

}  // namespace mongo
