/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/document_source_window_stub.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/document_source_feeder.h"
#include "streams/exec/message.h"
#include "streams/exec/planner.h"
#include <chrono>

namespace streams {

using namespace mongo;

StageConstraints DocumentSourceWindowStub::constraints(Pipeline::SplitState pipeState) const {
    return {StreamType::kBlocking,
            PositionRequirement::kNone,
            HostTypeRequirement::kNone,
            DiskUseRequirement::kNoDiskUse,
            FacetRequirement::kNotAllowed,
            TransactionRequirement::kNotAllowed,
            LookupRequirement::kNotAllowed,
            UnionRequirement::kNotAllowed};
}

std::list<boost::intrusive_ptr<DocumentSource>> DocumentSourceTumblingWindowStub::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return {make_intrusive<DocumentSourceTumblingWindowStub>(expCtx, elem.Obj().copy())};
}

std::list<boost::intrusive_ptr<DocumentSource>> DocumentSourceHoppingWindowStub::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return {make_intrusive<DocumentSourceHoppingWindowStub>(expCtx, elem.Obj().copy())};
}

std::list<boost::intrusive_ptr<DocumentSource>> DocumentSourceSessionWindowStub::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    return {make_intrusive<DocumentSourceSessionWindowStub>(expCtx, elem.Obj().copy())};
}


REGISTER_INTERNAL_DOCUMENT_SOURCE(tumblingWindow,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceTumblingWindowStub::createFromBson,
                                  true);

REGISTER_INTERNAL_DOCUMENT_SOURCE(hoppingWindow,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceHoppingWindowStub::createFromBson,
                                  true);

REGISTER_INTERNAL_DOCUMENT_SOURCE(sessionWindow,
                                  LiteParsedDocumentSourceDefault::parse,
                                  DocumentSourceSessionWindowStub::createFromBson,
                                  true);

ALLOCATE_DOCUMENT_SOURCE_ID(windowStub, DocumentSourceWindowStub::id)

}  // namespace streams
