/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <vector>

#include "mongo/bson/bsonobj.h"

namespace streams {

// Rewrites the input pipeline as necessary, records the rewrites performed.
class PipelineRewriter {
public:
    PipelineRewriter(std::vector<mongo::BSONObj> pipeline);

    // Rewrites the input pipeline.
    std::vector<mongo::BSONObj> rewrite();

    // Returns the $lookup stages that were rewritten.
    const std::vector<std::pair<mongo::BSONObj, mongo::BSONObj>>& getRewrittenLookupStages() const {
        return _rewrittenLookupStages;
    }

private:
    mongo::BSONObj rewriteLookUp(const mongo::BSONObj& stageObj);

    std::vector<mongo::BSONObj> _pipeline;
    // Tracks rewritten $lookup stages.
    std::vector<std::pair<mongo::BSONObj, mongo::BSONObj>> _rewrittenLookupStages;
};

};  // namespace streams
