/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/optimize.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/field_path.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/context.h"
#include "streams/exec/feature_flag.h"
#include "streams/exec/match_operator.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/exec/window_aware_operator.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace optimize {

bool Rule::checkPattern(const OperatorDag& dag) {
    tassert(mongo::ErrorCodes::InternalError, "Expected a non-empty DAG", !dag.operators().empty());
    return doCheckPattern(dag);
}

mongo::BSONObj Rule::setInternalField(mongo::BSONObj stage,
                                      mongo::StringData fieldName,
                                      auto value) {
    mongo::BSONObjBuilder stageSpec{stage.firstElement().Obj()};
    stageSpec.append(fieldName, std::move(value));
    return BSON(stage.firstElementFieldName() << stageSpec.obj());
}

std::vector<mongo::BSONObj> Rule::transform(OperatorDag dag) {
    return doTransform(std::move(dag));
}

std::vector<std::unique_ptr<Rule>> Rule::getRules(Context* context) {
    std::vector<std::unique_ptr<Rule>> rules;
    if (context->featureFlags->getFeatureFlagValue(FeatureFlags::kChangestreamPredicatePushdown)
            .isTrue()) {
        rules.push_back(std::make_unique<ChangestreamPredicatePushdown>(context));
    }
    return rules;
}

void Rule::assertExpectedStage(const std::vector<mongo::BSONObj>& plan,
                               size_t idx,
                               mongo::StringData name) {
    tassert(10072400,
            fmt::format("index of {} is too target for plan of length {}", idx, plan.size()),
            idx < plan.size());
    const auto& stage = plan[idx];
    tassert(10072401, "stage is empty", !stage.isEmpty());
    tassert(10072402,
            fmt::format(
                "stage has name {}, expected {}", stage.firstElementFieldNameStringData(), name),
            stage.firstElementFieldNameStringData() == name);
}

// Look for changestream $source followed by a $match.
bool ChangestreamPredicatePushdown::doCheckPattern(const OperatorDag& dag) {
    auto source = dynamic_cast<ChangeStreamSourceOperator*>(dag.operators().front().get());
    if (!source) {
        return false;
    }

    auto sourceOptions = source->getOptions();
    if (sourceOptions.fullDocumentOnly) {
        return false;
    }

    auto sourceOutput = source->getOutputInfo();
    tassert(mongo::ErrorCodes::InternalError,
            fmt::format("Expected {} to have one output", source->getName()),
            !sourceOutput.empty());

    auto match = dynamic_cast<MatchOperator*>(sourceOutput.front().oper);
    if (!match) {
        return false;
    }

    mongo::DepsTracker deps;
    auto depsState = match->documentSource()->getDependencies(&deps);
    if (depsState == mongo::DepsTracker::State::NOT_SUPPORTED) {
        LOGV2_INFO(
            10072403, "Unexpected DepsTracker::NOT_SUPPORTED result", "context"_attr = _context);
        return false;
    }
    if (deps.metadataDeps().test(mongo::DocumentMetadataFields::MetaType::kStream)) {
        return false;
    }
    if (_context->projectStreamMeta && deps.fields.contains(*_context->streamMetaFieldName)) {
        return false;
    }
    if (sourceOptions.timestampOutputFieldName &&
        deps.fields.contains(*sourceOptions.timestampOutputFieldName)) {
        return false;
    }

    // Don't push down the $match if there is a window in the pipeline.
    // This would filter out events that might advance the watermark.
    // TODO(SERVER-83298): Enable this for pipelines with $source.timeField unset.
    for (const auto& op : dag.operators()) {
        if (dynamic_cast<WindowAwareOperator*>(op.get())) {
            return false;
        }
    }

    return true;
}

std::vector<mongo::BSONObj> ChangestreamPredicatePushdown::doTransform(OperatorDag dag) {
    auto plan = std::move(dag.moveOptions().optimizedPipeline);
    assertExpectedStage(plan, 0, kSourceStageName);
    assertExpectedStage(plan, 1, mongo::DocumentSourceMatch::kStageName);
    mongo::BSONObjBuilder b{plan[0]};
    plan[0] = setInternalField(
        plan[0], mongo::ChangeStreamSourceOptions::kInternalPredicateFieldName, plan[1]);
    plan.erase(plan.begin() + 1);
    return plan;
}

}  // namespace optimize

}  // namespace streams
