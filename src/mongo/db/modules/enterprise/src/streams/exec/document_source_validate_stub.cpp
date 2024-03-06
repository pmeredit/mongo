/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <chrono>

#include "mongo/util/assert_util.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/document_source_validate_stub.h"
#include "streams/exec/message.h"
#include "streams/exec/parser.h"

namespace streams {

using namespace mongo;

StageConstraints DocumentSourceValidateStub::constraints(Pipeline::SplitState pipeState) const {
    return {StreamType::kStreaming,
            PositionRequirement::kNone,
            HostTypeRequirement::kNone,
            DiskUseRequirement::kNoDiskUse,
            FacetRequirement::kNotAllowed,
            TransactionRequirement::kNotAllowed,
            LookupRequirement::kNotAllowed,
            UnionRequirement::kNotAllowed};
}

std::list<boost::intrusive_ptr<DocumentSource>> DocumentSourceValidateStub::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return {make_intrusive<DocumentSourceValidateStub>(expCtx, elem.Obj())};
}

REGISTER_INTERNAL_DOCUMENT_SOURCE(validate,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceValidateStub::createFromBson,
                                  true);

}  // namespace streams
