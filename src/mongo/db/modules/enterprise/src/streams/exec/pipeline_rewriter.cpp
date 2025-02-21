/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/pipeline_rewriter.h"

#include "mongo/logv2/log.h"
#include "streams/exec/constants.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

PipelineRewriter::PipelineRewriter(std::vector<BSONObj> pipeline) {
    for (auto& stage : pipeline) {
        _pipeline.push_back(std::move(stage).getOwned());
    }
}

std::vector<BSONObj> PipelineRewriter::rewrite() {
    std::vector<BSONObj> rewrittenPipeline;
    for (const auto& stageObj : _pipeline) {
        auto stageName = stageObj.firstElement().fieldNameStringData();
        if (isLookUpStage(stageName)) {
            BSONObj newStageObj = rewriteLookUp(stageObj);
            _rewrittenLookupStages.push_back(std::make_pair(stageObj, newStageObj.copy()));
            rewrittenPipeline.push_back(std::move(newStageObj));
        } else {
            rewrittenPipeline.push_back(stageObj.copy());
        }
    }
    return rewrittenPipeline;
}

BSONObj PipelineRewriter::rewriteLookUp(const BSONObj& stageObj) {
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            str::stream() << "Invalid lookup spec: " << stageObj,
            isLookUpStage(stageObj.firstElementFieldName()) &&
                stageObj.firstElement().isABSONObj());

    auto lookupObj = stageObj.firstElement().Obj();
    auto fromField = lookupObj[kFromFieldName];
    if (!fromField) {
        // There is no rewrite to perform because the 'from' field does not exist.
        return stageObj;
    }
    uassert(ErrorCodes::StreamProcessorInvalidOptions,
            "The $lookup.from field must be an object",
            fromField.isABSONObj());

    // Remove the `connectionName` in the 'from' object in $lookup so that it can parsed with the
    // `allowGenericForeignDbLookup` flag.
    BSONObjBuilder newStageObjBuilder;
    BSONObjBuilder newLookupObjBuilder(newStageObjBuilder.subobjStart(kLookUpStageName));
    for (const auto& elem : lookupObj) {
        if (elem.fieldNameStringData() == kFromFieldName) {
            auto fromFieldObj = elem.embeddedObject();
            auto fromFieldObjNoConnectionName =
                fromFieldObj.removeField(AtlasCollection::kConnectionNameFieldName);
            newLookupObjBuilder.append(kFromFieldName, fromFieldObjNoConnectionName);
        } else {
            newLookupObjBuilder.append(elem);
        }
    }
    newLookupObjBuilder.doneFast();
    return newStageObjBuilder.obj();
}

};  // namespace streams
