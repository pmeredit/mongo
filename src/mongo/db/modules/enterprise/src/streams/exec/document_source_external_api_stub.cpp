/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/document_source_external_api_stub.h"
#include "streams/exec/planner.h"

namespace streams {

using namespace mongo;

StageConstraints DocumentSourceExternalApiStub::constraints(Pipeline::SplitState pipeState) const {
    return {StreamType::kStreaming,
            PositionRequirement::kNone,
            HostTypeRequirement::kNone,
            DiskUseRequirement::kNoDiskUse,
            FacetRequirement::kNotAllowed,
            TransactionRequirement::kNotAllowed,
            LookupRequirement::kNotAllowed,
            UnionRequirement::kNotAllowed};
}


std::list<boost::intrusive_ptr<DocumentSource>> DocumentSourceExternalApiStub::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return {make_intrusive<DocumentSourceExternalApiStub>(expCtx, elem.Obj().copy())};
}

mongo::Value DocumentSourceExternalApiStub::serialize(
    const mongo::SerializationOptions& opts) const {
    return Value(DOC(getSourceName() << Document(_bsonOptions)));
}


REGISTER_INTERNAL_DOCUMENT_SOURCE(externalAPI,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceExternalApiStub::createFromBson,
                                  true);

}  // namespace streams
