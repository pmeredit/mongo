/**
 *    Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/document_source_external_function_stub.h"
#include "streams/exec/planner.h"

namespace streams {

using namespace mongo;

StageConstraints DocumentSourceExternalFunctionStub::constraints(
    Pipeline::SplitState pipeState) const {
    return {StreamType::kStreaming,
            PositionRequirement::kNone,
            HostTypeRequirement::kNone,
            DiskUseRequirement::kNoDiskUse,
            FacetRequirement::kNotAllowed,
            TransactionRequirement::kNotAllowed,
            LookupRequirement::kNotAllowed,
            UnionRequirement::kNotAllowed};
}


std::list<boost::intrusive_ptr<DocumentSource>> DocumentSourceExternalFunctionStub::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return {make_intrusive<DocumentSourceExternalFunctionStub>(expCtx, elem.Obj().copy())};
}

mongo::Value DocumentSourceExternalFunctionStub::serialize(
    const mongo::SerializationOptions& opts) const {
    return Value(DOC(getSourceName() << Document(_bsonOptions)));
}


REGISTER_INTERNAL_DOCUMENT_SOURCE(externalFunction,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceExternalFunctionStub::createFromBson,
                                  true);
ALLOCATE_DOCUMENT_SOURCE_ID(externalFunction, DocumentSourceExternalFunctionStub::id)

}  // namespace streams
