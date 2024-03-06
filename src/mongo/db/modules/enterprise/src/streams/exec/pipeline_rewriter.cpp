/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/pipeline_rewriter.h"

#include "mongo/logv2/log.h"
#include "streams/exec/constants.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

std::vector<mongo::BSONObj> PipelineRewriter::rewrite() {
    std::vector<mongo::BSONObj> rewrittenPipeline;
    for (const auto& stageObj : _pipeline) {
        auto stageName = stageObj.firstElement().fieldNameStringData();
        if (isLookUpStage(stageName)) {
            auto newStageObj = rewriteLookUp(stageObj);
            _rewrittenLookupStages.push_back(std::make_pair(stageObj, newStageObj));
            rewrittenPipeline.push_back(std::move(newStageObj));
        } else {
            rewrittenPipeline.push_back(stageObj);
        }
    }
    return rewrittenPipeline;
}

BSONObj PipelineRewriter::rewriteLookUp(const BSONObj& stageObj) {
    uassert(ErrorCodes::InvalidOptions,
            str::stream() << "Invalid lookup spec: " << stageObj,
            isLookUpStage(stageObj.firstElementFieldName()) &&
                stageObj.firstElement().isABSONObj());

    auto lookupObj = stageObj.firstElement().Obj();
    auto fromField = lookupObj[kFromFieldName];
    uassert(ErrorCodes::FailedToParse, "must specify 'from' field for a $lookup", !fromField.eoo());

    // Rewrite the 'from' object in $lookup to be just a string containing the collection name.
    BSONObjBuilder newStageObjBuilder;
    BSONObjBuilder newLookupObjBuilder(newStageObjBuilder.subobjStart(kLookUpStageName));
    for (const auto& elem : lookupObj) {
        if (elem.fieldNameStringData() == kFromFieldName) {
            auto fromFieldObj = elem.embeddedObject();
            auto lookupFromAtlas =
                AtlasCollection::parse(IDLParserContext("AtlasCollection"), fromFieldObj);
            newLookupObjBuilder.append(kFromFieldName, lookupFromAtlas.getColl().toString());
        } else {
            newLookupObjBuilder.append(elem);
        }
    }
    newLookupObjBuilder.done();
    return newStageObjBuilder.obj();
}

};  // namespace streams
