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

#pragma once

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/dependencies.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_multi_stream_gen.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/stage_constraints.h"
#include "mongo/db/pipeline/variables.h"
#include "mongo/db/query/query_shape/serialization_options.h"
#include "mongo/util/intrusive_counter.h"

namespace mongo {

/**
 * The $betaMultiStream stage defines a mechanism by which one pipeline can handle two separate
 * document streams from two sub-pipelines (a "primary" and "secondary" pipeline). The stage
 * definition looks like:
 * { $betaMultiStream: {
 *     primary: [<pipeline>],
 *     secondary: [<pipeline>],
 *     finishMethod: one of "cursor" or "setVar"
 *   }
 * }
 *
 * The primary pipeline always feeds into the post-$betaMultiStream top-level pipeline. The handling
 * of the secondary pipeline is determined by the "finishMethod": either "cursor" or "setVar",
 * currently. If "cursor" method is used, the query will return a "meta" cursor with the results
 * from the secondary pipeline alongside the main "results" cursor. If "setVar" method is used, the
 * $$SEARCH_META variable holds the results from the secondary pipeline.
 *
 * This stage is used as container to define semantics for how one pipeline can handle multiple
 * streams of data during query planning, but is _not_ used for pipeline execution. Prior to
 * execution, this stage should always be transformed into a flat pipeline so that its doGetNext()
 * method is unreachable.
 *
 * The intended use case for this stage is for executing a secondary stream that populates a
 * variable. On a single node, the stage is easily transformed with $setVariableFromSubpipeline. On
 * a sharded query, the merging node dispatches a "cursor"-style $betaMultiStream to the shards
 * and inserts a $setVariableFromSubpipeline to read from the secondary meta cursor in the merging
 * pipeline.
 *
 * IMPORTANT: This stage is written for a PoC of the Extensions API (specifically to PoC sharded
 * faceting for $search). The current implementation is somewhat hacky and crude. When implemented
 * for production use, we should make a meaningful effort for the aggregation path to handle
 * multiple streams more elegantly.
 */
class DocumentSourceMultiStream final : public DocumentSource {
public:
    static constexpr StringData kStageName = "$betaMultiStream"_sd;

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& expCtx);

    DocumentSourceMultiStream(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                              std::vector<BSONObj> primaryPipeline,
                              std::vector<BSONObj> secondaryPipeline,
                              SecondaryStreamFinishMethodEnum finishMethod)
        : DocumentSource(kStageName, expCtx),
          _primaryPipeline(Pipeline::parse(primaryPipeline, expCtx)),
          _secondaryPipeline(Pipeline::parse(secondaryPipeline, expCtx)),
          _finishMethod(finishMethod) {}

    ~DocumentSourceMultiStream() override = default;

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final;

    const char* getSourceName() const final {
        return kStageName.rawData();
    }

    static const Id& id;

    Id getId() const override {
        return id;
    }

    StageConstraints constraints(Pipeline::SplitState) const final {
        // TODO Consider which constraints should check subpipeline constraints.
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kNotAllowed,
                                     UnionRequirement::kNotAllowed);
        constraints.requiresInputDocSource = false;
        return constraints;
    }

    void setFinishMethod(SecondaryStreamFinishMethodEnum finishMethod) {
        _finishMethod = finishMethod;
    }

    SecondaryStreamFinishMethodEnum getFinishMethod() const {
        return _finishMethod;
    }

    /**
     * Transfers ownership of the primary pipeline to the caller.
     */
    std::unique_ptr<Pipeline, PipelineDeleter> getPrimaryPipeline() {
        return std::move(_primaryPipeline);
    }

    /**
     * Transfers ownership of the secondary pipeline to the caller.
     */
    std::unique_ptr<Pipeline, PipelineDeleter> getSecondaryPipeline() {
        return std::move(_secondaryPipeline);
    }

    DepsTracker::State getDependencies(DepsTracker* deps) const final;

    void addVariableRefs(std::set<Variables::Id>* refs) const final;

private:
    GetNextResult doGetNext() final;
    Value serialize(const SerializationOptions& opts = SerializationOptions{}) const final;

    std::unique_ptr<Pipeline, PipelineDeleter> _primaryPipeline;
    std::unique_ptr<Pipeline, PipelineDeleter> _secondaryPipeline;
    SecondaryStreamFinishMethodEnum _finishMethod;
};
}  // namespace mongo
