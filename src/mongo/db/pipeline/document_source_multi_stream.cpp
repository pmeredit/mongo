/**
 *    Copyright (C) 2025-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/pipeline/document_source_multi_stream.h"

#include "mongo/db/pipeline/document_source_set_variable_from_subpipeline.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

namespace mongo {

using boost::intrusive_ptr;

constexpr StringData DocumentSourceMultiStream::kStageName;

REGISTER_DOCUMENT_SOURCE(betaMultiStream,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceMultiStream::createFromBson,
                         AllowedWithApiStrict::kNeverInVersion1);

ALLOCATE_DOCUMENT_SOURCE_ID(betaMultiStream, DocumentSourceMultiStream::id);

namespace {
static std::pair<boost::optional<BSONObj>, std::list<boost::intrusive_ptr<DocumentSource>>>
getSubPipelineMergingLogic(const Pipeline& pipeline) {
    boost::optional<BSONObj> mergeSortPattern;
    std::list<boost::intrusive_ptr<DocumentSource>> mergingStages = {};
    for (auto& stage : pipeline.getSources()) {
        auto stageLogic = stage->distributedPlanLogic();
        // If the stage has no DistributedPlanLogic, it will get sent to the shards pipeline without
        // needing to modify the merging logic.
        if (!stageLogic) {
            continue;
        }
        tassert(12345,
                "A stage used in a $betaMultiStream subpipeline must be able to be sent to shards",
                stageLogic->shardsStage != nullptr);
        // The first merge sort pattern seen takes precedence.
        if (stageLogic->mergeSortPattern && !mergeSortPattern) {
            mergeSortPattern = stageLogic->mergeSortPattern;
        }

        // Add the subpipeline stage's merging stages.
        mergingStages.splice(mergingStages.end(), stageLogic->mergingStages);
    }

    return {mergeSortPattern, mergingStages};
}
};  // namespace

Value DocumentSourceMultiStream::serialize(const SerializationOptions& opts) const {
    MultiStreamSpec spec;
    spec.setPrimary(_primaryPipeline->serializeToBson(opts));
    spec.setSecondary(_secondaryPipeline->serializeToBson(opts));
    spec.setFinishMethod(_finishMethod);
    return Value(DOC(getSourceName() << spec.toBSON()));
}

DepsTracker::State DocumentSourceMultiStream::getDependencies(DepsTracker* deps) const {
    return DepsTracker::State::NOT_SUPPORTED;
}

void DocumentSourceMultiStream::addVariableRefs(std::set<Variables::Id>* refs) const {}

boost::optional<DocumentSource::DistributedPlanLogic>
DocumentSourceMultiStream::distributedPlanLogic() {
    tassert(12345,
            "Distributed planning for $betaMultiStream is currently only enabled for finishMethod "
            "\"setVar\"",
            _finishMethod == SecondaryStreamFinishMethodEnum::kSetVar);

    DistributedPlanLogic logic;

    // The $multiCursor will be sent to the shards with "cursor" finish method.
    auto shardsMultiCursorStage = boost::intrusive_ptr<DocumentSourceMultiStream>(
        static_cast<DocumentSourceMultiStream*>(clone(pExpCtx).get()));
    shardsMultiCursorStage->setFinishMethod(SecondaryStreamFinishMethodEnum::kCursor);
    logic.shardsStage = std::move(shardsMultiCursorStage);

    // The merging pipeline will look like [<primary pipeline merging stages>,
    // {$setVariableFromSubPipeline: [<secondary pipeline merging stages>]}]; and the sort pattern
    // will be taken from the primary pipeline.
    auto primaryPipelineMergingLogic = getSubPipelineMergingLogic(*_primaryPipeline);
    logic.mergeSortPattern = primaryPipelineMergingLogic.first;
    logic.mergingStages = std::move(primaryPipelineMergingLogic.second);

    auto secondaryPipelineMergingLogic = getSubPipelineMergingLogic(*_secondaryPipeline);
    logic.mergingStages.push_back(DocumentSourceSetVariableFromSubPipeline::create(
        pExpCtx,
        Pipeline::create(std::move(secondaryPipelineMergingLogic.second), pExpCtx),
        Variables::kSearchMetaId));

    return logic;
}

boost::intrusive_ptr<DocumentSource> DocumentSourceMultiStream::createFromBson(
    BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx) {
    uassert(ErrorCodes::FailedToParse,
            str::stream() << "The " << kStageName
                          << " stage specification must be an object, found "
                          << typeName(elem.type()),
            elem.type() == BSONType::Object);

    auto spec = MultiStreamSpec::parse(IDLParserContext(kStageName), elem.embeddedObject());
    return make_intrusive<DocumentSourceMultiStream>(expCtx,
                                                     std::move(spec.getPrimary()),
                                                     std::move(spec.getSecondary()),
                                                     spec.getFinishMethod());
}

DocumentSource::GetNextResult DocumentSourceMultiStream::doGetNext() {
    MONGO_UNREACHABLE_TASSERT(1234500);
}
}  // namespace mongo
