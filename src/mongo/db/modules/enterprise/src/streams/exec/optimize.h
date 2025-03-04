/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <string>

#include "streams/exec/operator_dag.h"
#include "streams/exec/stream_processor_feature_flags.h"

namespace streams {

struct Context;

namespace optimize {

// Rule defines an optimization rule. The Planner can apply one or more Rules to an OperatorDag.
// The Planner checks if a rule applies to an OperatorDag using the checkPattern method.
// If the rule applies, the transform method is used to get a new execution plan with the
// rule applied.
//
// The checkPattern and transform methods are inspired by the Cascades optimization framework
// described in
// https://www.microsoft.com/en-us/research/uploads/prod/2024/12/Extensible-Query-Optimizers-in-Practice.pdf
//
// NOTE: Before we add more optimization rules to this, we should consider the following
// alternative:
//  1. Add DocumentSource*Stub classes for $source and sink stages.
//  2. Restructure planner.cpp to parse the entire user pipeline with Pipeline::parse. Currently, we
//  parse the source and sinks stages separately from the middle pipeline.
//  3. Add optimization rules within the DocumentSource*Stub::optimize methods.
// This alternative might be cleaner as it extends the optimization infrastructure used in Pipeline
// and DocumentSource classes. But it requires a lot of changes to streams planner.cpp so we avoid
// it for now.
class Rule {
public:
    Rule(Context* context) : _context(context) {}

    virtual ~Rule() {}

    // Returns all optimization rules for this set of feature flags.
    static std::vector<std::unique_ptr<Rule>> getRules(Context* context);

    // Returns true if this rule applies to this dag.
    bool checkPattern(const OperatorDag& dag);
    // Applies this Rule to this DAG, returns a new execution plan.
    std::vector<mongo::BSONObj> transform(OperatorDag dag);
    // Returns the name of this rule.
    virtual std::string name() = 0;

protected:
    virtual bool doCheckPattern(const OperatorDag& dag) = 0;
    virtual std::vector<mongo::BSONObj> doTransform(OperatorDag dag) = 0;

    void assertExpectedStage(const std::vector<mongo::BSONObj>& plan,
                             size_t idx,
                             mongo::StringData name);
    mongo::BSONObj setInternalField(mongo::BSONObj stage, mongo::StringData fieldName, auto value);

    Context* _context{nullptr};
};

// ChangestreamPredicatePushdown is used to pushdown a $match following a changestream
// $source stage.
class ChangestreamPredicatePushdown : public Rule {
public:
    ChangestreamPredicatePushdown(Context* context) : Rule(context) {}

    bool doCheckPattern(const OperatorDag& dag) override;
    std::vector<mongo::BSONObj> doTransform(OperatorDag dag) override;
    std::string name() override {
        return "ChangestreamPredicatePushdown";
    }
};

}  // namespace optimize

}  // namespace streams
