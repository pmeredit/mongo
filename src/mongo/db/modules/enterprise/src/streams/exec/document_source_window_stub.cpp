/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/document_source_window_stub.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/parser.h"
#include <chrono>

namespace streams {

using namespace mongo;

StageConstraints DocumentSourceWindowStub::constraints(Pipeline::SplitState pipeState) const {
    return {StreamType::kBlocking,
            PositionRequirement::kNone,
            HostTypeRequirement::kLocalOnly,
            DiskUseRequirement::kWritesTmpData,
            FacetRequirement::kNotAllowed,
            TransactionRequirement::kNotAllowed,
            LookupRequirement::kNotAllowed,
            UnionRequirement::kNotAllowed,
            ChangeStreamRequirement::kDenylist};
}

std::list<boost::intrusive_ptr<DocumentSource>> DocumentSourceWindowStub::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return {make_intrusive<DocumentSourceWindowStub>(expCtx, elem.Obj())};
}

REGISTER_INTERNAL_DOCUMENT_SOURCE(tumblingWindow,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceWindowStub::createFromBson,
                                  true);

}  // namespace streams
