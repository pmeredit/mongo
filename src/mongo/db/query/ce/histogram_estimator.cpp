/**
 *    Copyright (C) 2022-present MongoDB, Inc.
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

#include "mongo/db/query/ce/histogram_estimator.h"

namespace mongo::ce {

Cardinality HistogramEstimator::estimateCardinality(const stats::CEHistogram& hist,
                                                    const Cardinality collectionSize,
                                                    const mongo::Interval& interval,
                                                    bool includeScalar) {
    // Rescales the cardinality according to the current collection size.
    return (estimateIntervalCardinality(hist, interval, includeScalar) / hist.getSampleSize()) *
        collectionSize;
}

bool HistogramEstimator::canEstimateInterval(const stats::CEHistogram& hist,
                                             const mongo::Interval& interval,
                                             bool includeScalar) {

    auto [startTag, startVal] = sbe::bson::convertFrom<false>(interval.start);
    auto [endTag, endVal] = sbe::bson::convertFrom<false>(interval.end);
    sbe::value::ValueGuard startGuard{startTag, startVal};
    sbe::value::ValueGuard endGuard{endTag, endVal};

    // If 'startTag' and 'endTag' are either in the same type or type-bracketed, they are estimable
    // directly via either histograms or type counts.
    if (stats::sameTypeBracketedInterval(startTag, interval.endInclusive, endTag, endVal)) {
        // TODO: SERVER-91639 to support estimating via type counts here.
        return stats::canEstimateTypeViaHistogram(startTag);
    }

    return false;
}

}  // namespace mongo::ce
