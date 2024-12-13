/**
 *    Copyright (C) 2024-present MongoDB, Inc.
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

#include "mongo/db/query/cost_based_ranker/cardinality_estimator.h"

#include "mongo/db/query/ce/histogram_estimator.h"
#include "mongo/db/query/cost_based_ranker/heuristic_estimator.h"
#include "mongo/db/query/index_bounds_builder.h"
#include "mongo/db/query/stage_types.h"

namespace mongo::cost_based_ranker {

CardinalityEstimator::CardinalityEstimator(const stats::CollectionStatistics& collStats,
                                           const ce::SamplingEstimator* samplingEstimator,
                                           EstimateMap& qsnEstimates,
                                           QueryPlanRankerModeEnum rankerMode)
    : _collCard{CardinalityEstimate{CardinalityType{collStats.getCardinality()},
                                    EstimationSource::Metadata}},
      _inputCard{_collCard},
      _collStats(collStats),
      _samplingEstimator(samplingEstimator),
      _qsnEstimates{qsnEstimates},
      _rankerMode(rankerMode) {
    if (_rankerMode == QueryPlanRankerModeEnum::kSamplingCE ||
        _rankerMode == QueryPlanRankerModeEnum::kAutomaticCE) {
        tassert(9746501,
                "samplingEstimator cannot be null when ranker mode is samplingCE or automaticCE",
                _samplingEstimator != nullptr);
    }
}

CEResult CardinalityEstimator::estimate(const QuerySolutionNode* node) {
    StageType nodeType = node->getType();
    CEResult ceRes{zeroCE};
    bool isConjunctionBreaker = false;

    switch (nodeType) {
        case STAGE_COLLSCAN:
            ceRes = estimate(static_cast<const CollectionScanNode*>(node));
            break;
        case STAGE_VIRTUAL_SCAN:
            ceRes = estimate(static_cast<const VirtualScanNode*>(node));
            break;
        case STAGE_IXSCAN:
            ceRes = estimate(static_cast<const IndexScanNode*>(node));
            break;
        case STAGE_FETCH:
            ceRes = estimate(static_cast<const FetchNode*>(node));
            break;
        case STAGE_AND_HASH:
            ceRes = indexIntersectionCard(static_cast<const AndHashNode*>(node));
            break;
        case STAGE_AND_SORTED:
            ceRes = indexIntersectionCard(static_cast<const AndSortedNode*>(node));
            break;
        case STAGE_OR:
            ceRes = indexUnionCard(static_cast<const OrNode*>(node));
            isConjunctionBreaker = true;
            break;
        case STAGE_SORT_MERGE:
            ceRes = indexUnionCard(static_cast<const MergeSortNode*>(node));
            isConjunctionBreaker = true;
            break;
        case STAGE_SORT_DEFAULT:
        case STAGE_SORT_SIMPLE:
        case STAGE_PROJECTION_DEFAULT:
        case STAGE_PROJECTION_COVERED:
        case STAGE_PROJECTION_SIMPLE:
            ceRes = passThroughNodeCard(node);
            break;
        default:
            MONGO_UNIMPLEMENTED_TASSERT(9586709);
    }

    if (!ceRes.isOK()) {
        return ceRes;
    }
    if (isConjunctionBreaker) {
        tassert(9586705,
                "All indirect conjuncts should have been taken into account.",
                _conjSels.empty());
        _inputCard = ceRes.getValue();
    }

    return ceRes;
}

CEResult CardinalityEstimator::estimate(const MatchExpression* node, const bool isFilterRoot) {
    if (isFilterRoot && _rankerMode == QueryPlanRankerModeEnum::kSamplingCE) {
        // Sample the entire filter
        return _samplingEstimator->estimateCardinality(node);
    }

    const MatchExpression::MatchType nodeType = node->matchType();
    CEResult ceRes{zeroCE};

    switch (nodeType) {
        case MatchExpression::EQ:
        case MatchExpression::LT:
        case MatchExpression::LTE:
        case MatchExpression::GT:
        case MatchExpression::GTE:
            ceRes = estimate(static_cast<const ComparisonMatchExpression*>(node));
            break;
        case MatchExpression::AND:
            ceRes = estimate(static_cast<const AndMatchExpression*>(node));
            break;
        case MatchExpression::OR:
            ceRes = estimate(static_cast<const OrMatchExpression*>(node));
            break;
        default:
            if (node->numChildren() == 0) {
                ceRes = estimate(static_cast<const LeafMatchExpression*>(node));
            } else {
                MONGO_UNIMPLEMENTED_TASSERT(9586708);
            }
    }

    if (!ceRes.isOK()) {
        return ceRes;
    }
    if (isFilterRoot && (node->numChildren() == 0 || nodeType == MatchExpression::OR)) {
        // Leaf nodes and ORs that are the root of a QSN's filter are atomic from conjunction
        // estimation's perspective, therefore conjunction estimation will not add such nodes
        // to the _conjSels stack. Here we add the selectivity of such root nodes to _conjSels
        // so that they participate in implicit conjunction selectivity calculation. For instance
        // a plan of an IndexScanNode with a filter (a < 5) OR (a > 10), and a subsequent FetchNode
        // with a filter (b > 'abc') express a conjunction ((a < 5) OR (a > 10) AND (b > 'abc')).
        // Both conjuncts are added here when each QSN estimates its filter node.
        // All conjuncts' selectivities are combined when computing the total cardinality of the
        // FetchNode.
        SelectivityEstimate sel = ceRes.getValue() / _inputCard;
        _conjSels.emplace_back(sel);
    }

    return ceRes;
}

/*
 * QuerySolutionNodes
 */
CEResult CardinalityEstimator::estimate(const CollectionScanNode* node) {
    return scanCard(node, _inputCard);
}

CEResult CardinalityEstimator::estimate(const VirtualScanNode* node) {
    CardinalityEstimate virtualCollCard{CardinalityType{(double)node->docs.size()},
                                        EstimationSource::Code};
    return scanCard(node, virtualCollCard);
}

CEResult CardinalityEstimator::scanCard(const QuerySolutionNode* node,
                                        const CardinalityEstimate& card) {
    QSNEstimate est;
    est.inCE = card;
    if (const MatchExpression* filter = node->filter.get()) {
        auto ceRes = estimate(filter, true);
        if (!ceRes.isOK()) {
            return ceRes;
        }
        est.filterCE = ceRes.getValue();
        est.outCE = *est.filterCE;
    } else {
        est.outCE = card;
    }
    CardinalityEstimate outCE{est.outCE};
    _qsnEstimates.emplace(node, std::move(est));

    return outCE;
}

CEResult CardinalityEstimator::estimate(const IndexScanNode* node) {
    QSNEstimate est;
    // Ignore selectivities pushed by other operators up to this point
    size_t selOffset = _conjSels.size();

    // Estimate the number of keys in the scan's interval.
    auto ceRes1 = estimate(&node->bounds);
    if (!ceRes1.isOK()) {
        return ceRes1;
    }
    est.inCE = ceRes1.getValue();

    if (const MatchExpression* filter = node->filter.get()) {
        // Notice that filterCE is estimated independent of interval CE.
        auto ceRes2 = estimate(filter, true);
        if (!ceRes2.isOK()) {
            return ceRes2;
        }
        est.filterCE = ceRes2.getValue();
    }

    // Estimate the cardinality of the combined index scan and filter conditions.
    // TODO: conjCard doesn't account for double-counting because some of the filter conditions
    // may re-evaluate the interval bounds.
    est.outCE = conjCard(selOffset, _inputCard);
    CardinalityEstimate outCE{est.outCE};
    _qsnEstimates.emplace(node, std::move(est));

    return outCE;
}

CEResult CardinalityEstimator::estimate(const FetchNode* node) {
    QSNEstimate est;

    tassert(
        9586704, "There cannot be other sub-plans parallel to a FetchNode", _conjSels.size() == 0);

    // Child's result CE is the input CE of this node, so there is no entry for it for this node.
    auto ceRes1 = estimate(node->children[0].get());
    if (!ceRes1.isOK()) {
        return ceRes1;
    }

    if (const MatchExpression* filter = node->filter.get()) {
        auto ceRes2 = estimate(filter, true);
        if (!ceRes2.isOK()) {
            return ceRes2;
        }
        est.filterCE = ceRes2.getValue();
    }

    tassert(9768403,
            "FetchNode must have direct or indirect children that somehow filter data.",
            _conjSels.size() > 0);

    // Combine the selectivity of this node's filter (if any) with its child selectivities.
    est.outCE = conjCard(0, _inputCard);
    CardinalityEstimate outCE{est.outCE};
    _qsnEstimates.emplace(node, std::move(est));

    return outCE;
}

CEResult CardinalityEstimator::passThroughNodeCard(const QuerySolutionNode* node) {
    tassert(9768401, "Pass-through nodes cannot have filters.", !node->filter);
    tassert(9768402, "Pass-through nodes must have a single child.", node->children.size() == 1);
    QSNEstimate est;
    est.outCE = estimate(node->children[0].get()).getValue();
    CardinalityEstimate outCE{est.outCE};
    _qsnEstimates.emplace(node, std::move(est));
    return outCE;
}

template <IntersectionType T>
CEResult CardinalityEstimator::indexIntersectionCard(const T* node) {
    tassert(9586703, "Index intersection nodes are not expected to have filters.", !node->filter);

    QSNEstimate est;
    // Ignore selectivities pushed by other operators up to this point.
    size_t selOffset = _conjSels.size();

    for (auto&& child : node->children) {
        auto ceRes = estimate(child.get());
        if (!ceRes.isOK()) {
            return ceRes;
        }
    }

    // Combine the selectivities of all child nodes.
    est.outCE = conjCard(selOffset, _inputCard);
    CardinalityEstimate outCE{est.outCE};
    _qsnEstimates.emplace(node, std::move(est));

    return outCE;
}

template <UnionType T>
CEResult CardinalityEstimator::indexUnionCard(const T* node) {
    tassert(9586701, "Index union nodes are not expected to have filters.", !node->filter);

    QSNEstimate est;

    std::vector<SelectivityEstimate> disjSels;
    size_t selOffset = _conjSels.size();
    // We do not support intersections of unions
    tassert(9586702, "Currently index union is a top-level node.", selOffset == 0);
    for (auto&& child : node->children) {
        auto ceRes = estimate(child.get());
        if (!ceRes.isOK()) {
            return ceRes;
        }
        trimSels(selOffset);
        disjSels.emplace_back(ceRes.getValue() / _inputCard);
    }

    // Combine the selectivities of all child nodes.
    est.outCE = disjCard(_inputCard, disjSels);
    CardinalityEstimate outCE{est.outCE};
    _qsnEstimates.emplace(node, std::move(est));

    return outCE;
}

// Estimate the CE of a conjunction of expressions in 'nodes'. This function assumes that:
// * 'nodes' contains only ComparisonMatchExpressions.
// * 'path' corresponds to the path of all MatchExpressions in 'nodes'.
// It works by building an OrderedIntervalList representing the bounds that would be generated for a
// hypothetical index on 'path' for the conjunction of expressions in 'nodes' and then invoking
// histogram estimation for the resulting OIL.
CEResult CardinalityEstimator::estimateConjWithHistogram(
    StringData path, const std::vector<const MatchExpression*>& nodes) {
    // Note: We are assuming that the field is non-multikey when building this interval.
    // TODO SERVER-98374: Once CBR has catalog information, bail out of histogram estimation here if
    // 'path' is multikey.
    IndexEntry fakeIndex(BSON(path << "1") /* keyPattern */,
                         INDEX_BTREE,
                         IndexDescriptor::IndexVersion::kV2,
                         false /* multikey */,
                         {} /* multikeyPaths */,
                         {} /* multikeyPathSet */,
                         false /* sparse */,
                         false /* unique */,
                         CoreIndexInfo::Identifier("idx"),
                         nullptr /* filterExpression */,
                         BSONObj::kEmptyObject /* infoObj */,
                         nullptr /* collatorInterface */,
                         nullptr /* wildcardProjection */);
    OrderedIntervalList oil;
    IndexBoundsBuilder::allValuesForField(fakeIndex.keyPattern.firstElement(), &oil);
    IndexBoundsBuilder::BoundsTightness tightness;

    // For each node, translate to OIL and accumulate their intersection.
    for (auto&& expr : nodes) {
        IndexBoundsBuilder::translateAndIntersect(
            expr, fakeIndex.keyPattern.firstElement(), fakeIndex, &oil, &tightness, nullptr);
    }

    if (!(tightness == IndexBoundsBuilder::EXACT ||
          tightness == IndexBoundsBuilder::EXACT_MAYBE_COVERED)) {
        return Status(
            ErrorCodes::HistogramCEFailure,
            str::stream{}
                << "encountered unimplemented case where index bounds tightness are non-exact: "
                << oil.toString(false));
    }
    return estimate(&oil, true);
}

CEResult CardinalityEstimator::tryHistogramAnd(const AndMatchExpression* node) {
    // Set of unique paths references under 'node'
    StringDataSet paths;
    // Map from path to set of MatchExpression* referencing that path
    std::unordered_multimap<StringData,  // NOLINT
                            const MatchExpression*,
                            StringMapHasher,
                            StringMapEq>
        exprsByPath;

    // Iterate over the children of this AndMatchExpression and perform the following:
    // 1. Verify all children of this AndMatchExpression are ComparisonMatchExpressions and thus can
    // be converted to index bounds.
    // 2. Keep track of unique paths.
    // 3. Keep track of mapping from path to children which reference that path.
    for (size_t i = 0; i < node->numChildren(); ++i) {
        const auto child = node->getChild(i);
        if (dynamic_cast<const ComparisonMatchExpression*>(child) == nullptr) {
            return CEResult(ErrorCodes::HistogramCEFailure,
                            str::stream{} << "encountered child of AndMatchExpression that was not "
                                             "sargable (ComparisonMatchExpression): "
                                          << child->toString());
        }
        paths.insert(child->path());
        exprsByPath.insert({child->path(), child});
    }

    size_t selOffset = _conjSels.size();

    for (auto&& path : paths) {
        // Set of expressions referencing the current path
        std::vector<const MatchExpression*> nodesForPath;
        auto [start, end] = exprsByPath.equal_range(path);
        std::transform(start, end, std::back_inserter(nodesForPath), [](const auto& element) {
            return element.second;
        });
        // Estimate conjunction of all expressions for the current path
        auto ceRes = estimateConjWithHistogram(path, nodesForPath);
        if (!ceRes.isOK()) {
            return ceRes;
        }
        _conjSels.emplace_back(ceRes.getValue() / _inputCard);
    }
    // Combine CE's for the estimates of all paths under 'node'.
    return conjCard(selOffset, _inputCard);
}

/*
 * MatchExpressions
 */
CEResult CardinalityEstimator::estimate(const ComparisonMatchExpression* node) {
    bool fallbackToHeuristicCE = false;
    bool strict = _rankerMode == QueryPlanRankerModeEnum::kHistogramCE;

    // We can use a histogram to estimate this MatchExpression by constructing the corresponding
    // interval and estimating that.
    if (_rankerMode == QueryPlanRankerModeEnum::kHistogramCE ||
        _rankerMode == QueryPlanRankerModeEnum::kAutomaticCE) {
        auto ceRes = estimateConjWithHistogram(node->path(), {node});
        if (ceRes.isOK() || strict) {
            return ceRes;
        }
        fallbackToHeuristicCE = true;
    }

    if (_rankerMode == QueryPlanRankerModeEnum::kHeuristicCE || fallbackToHeuristicCE) {
        const SelectivityEstimate sel = estimateLeafMatchExpression(node, _inputCard);
        return sel * _inputCard;
    }

    MONGO_UNREACHABLE_TASSERT(9751900);
}

CEResult CardinalityEstimator::estimate(const LeafMatchExpression* node) {
    const SelectivityEstimate sel = estimateLeafMatchExpression(node, _inputCard);
    return sel * _inputCard;
}

CEResult CardinalityEstimator::estimate(const AndMatchExpression* node) {
    // Find with an empty query "coll.find({})" generates a AndMatchExpression without children.
    if (node->numChildren() == 0) {
        return _inputCard;
    }

    // Try to use histograms to estimate all children of this AndMatchExpression.
    // TODO: Suppose we have an AND with some predicates on 'a' that can answered with a
    // histogram and some predicates on 'b' that can't. Should we still try to use histogram for
    // 'a'? The code as written will not.
    if (_rankerMode == QueryPlanRankerModeEnum::kHistogramCE) {
        return tryHistogramAnd(node);
    } else if (_rankerMode == QueryPlanRankerModeEnum::kAutomaticCE) {
        auto ceRes = tryHistogramAnd(node);
        if (ceRes.isOK()) {
            return ceRes.getValue();
        }
        // Fallback to alternate AndMatchExpression estimation.
    }

    // Ignore selectivities pushed by other operators up to this point.
    size_t selOffset = _conjSels.size();

    for (size_t i = 0; i < node->numChildren(); i++) {
        auto ceRes = estimate(node->getChild(i), false);
        if (!ceRes.isOK()) {
            return ceRes;
        }
        // Add the child selectivity to the conjunction selectivity stack so that it can be
        // combined with selectivities of conjuncts or leaf nodes from parent QSNs.
        SelectivityEstimate sel = ceRes.getValue() / _inputCard;
        _conjSels.emplace_back(sel);
    }

    // Notice that the resulting selectivity is not being pushed onto the _conjSels stack
    // because it would otherwise result in double counting when computing the parent selectivity.
    return conjCard(selOffset, _inputCard);
}

CEResult CardinalityEstimator::estimate(const OrMatchExpression* node) {
    tassert(9586706, "OrMatchExpression must have children.", node->numChildren() > 0);
    std::vector<SelectivityEstimate> disjSels;
    size_t selOffset = _conjSels.size();
    for (size_t i = 0; i < node->numChildren(); i++) {
        auto ceRes = estimate(node->getChild(i), false);
        if (!ceRes.isOK()) {
            return ceRes;
        }
        trimSels(selOffset);
        disjSels.emplace_back(ceRes.getValue() / _inputCard);
    }
    return disjCard(_inputCard, disjSels);
}

/*
 * Intervals
 */
CEResult CardinalityEstimator::estimate(const IndexBounds* node) {
    if (node->isSimpleRange) {
        MONGO_UNIMPLEMENTED_TASSERT(9586707);  // TODO: SERVER-96816
    }
    if (node->isUnbounded()) {
        _conjSels.emplace_back(oneSel);
        return _inputCard;
    }

    // Iterate over all intervals over individual index fields (OILs). These intervals are
    // partitioned into two groups.
    // - The first group consists of a prefix of point intervals(equalities) possibly followed by a
    // singe range (inequality). These intervals allow to form a pair of start and end keys used to
    // scan the index. The number of keys between the start and end keys define the cost of the
    // index scan itself.
    // - The second group of intervals could be considered to be applied against the matching index
    // keys to filter them out. We call these the "residual" conditions.
    //
    // The combined CE of all index scan intervals are used to estimate the number of matching
    // keys passed to the parent QSN node. This partitioning of intervals provides an upper bound
    // to the number of keys scanned by the "recursive index navigation" strategy.
    // Example - given the index bounds: {[3,3], [42,42], ['a','z'], [13,13], ['df','jz']}
    // The "equality prefix is: {[3,3], [42,42], ['a','z']}
    // The residual intervals (conditions) are: {[13,13], ['df','jz']}
    std::vector<SelectivityEstimate> residualSels;
    bool isEqPrefix = true;  // Tracks if an OIL is part of an equality prefix
    // Ignore selectivities pushed by other operators up to this point.
    size_t selOffset = _conjSels.size();
    for (const auto& field : node->fields) {
        const OrderedIntervalList* oil = &field;
        // Notice that OILs are considered leaves from CE perspective.
        auto ceRes = estimate(oil);
        if (!ceRes.isOK()) {
            return ceRes;
        }
        SelectivityEstimate sel = ceRes.getValue() / _inputCard;
        if (isEqPrefix) {
            _conjSels.emplace_back(sel);
        } else {
            residualSels.emplace_back(sel);
        }
        isEqPrefix = isEqPrefix && oil->isPoint();
    }

    auto res = conjCard(selOffset, _inputCard);
    for (const auto& sel : residualSels) {
        _conjSels.emplace_back(std::move(sel));
    }
    return res;
}

CEResult CardinalityEstimator::estimate(const OrderedIntervalList* node, bool forceHistogram) {
    auto localRankerMode = _rankerMode;
    const bool strict = _rankerMode == QueryPlanRankerModeEnum::kHistogramCE || forceHistogram;
    const stats::CEHistogram* histogram = nullptr;

    if (_rankerMode == QueryPlanRankerModeEnum::kHistogramCE ||
        _rankerMode == QueryPlanRankerModeEnum::kAutomaticCE || forceHistogram) {
        histogram = _collStats.getHistogram(node->name);
        if (!histogram) {
            if (strict) {
                return CEResult(ErrorCodes::HistogramCEFailure,
                                str::stream{} << "no histogram found for path: " << node->name);
            }
            localRankerMode = QueryPlanRankerModeEnum::kHeuristicCE;
        }
    }

    // The intervals in an OIL are disjunct by definition, therefore the total cardinality is
    // the sum of cardinalities of the intervals. Therefore interval selectivities are summed.
    CardinalityEstimate resultCard = minCE;
    for (const auto& interval : node->intervals) {
        bool fallbackToHeuristicCE = false;
        if (localRankerMode == QueryPlanRankerModeEnum::kHistogramCE ||
            localRankerMode == QueryPlanRankerModeEnum::kAutomaticCE) {
            if (ce::HistogramEstimator::canEstimateInterval(*histogram, interval)) {
                resultCard += ce::HistogramEstimator::estimateCardinality(
                    *histogram,
                    _inputCard,
                    interval,
                    true,
                    ce::ArrayRangeEstimationAlgo::kExactArrayCE);
            } else {
                if (strict) {
                    return CEResult(ErrorCodes::HistogramCEFailure,
                                    str::stream{} << "encountered interval which is unestimatable: "
                                                  << interval.toString(true));
                }
                fallbackToHeuristicCE = true;
            }
        }
        if (localRankerMode == QueryPlanRankerModeEnum::kHeuristicCE || fallbackToHeuristicCE) {
            SelectivityEstimate sel = estimateInterval(interval, _inputCard);
            resultCard += sel * _inputCard;
        }
    }

    resultCard = std::min(resultCard, _inputCard);
    return resultCard;
}

}  // namespace mongo::cost_based_ranker
