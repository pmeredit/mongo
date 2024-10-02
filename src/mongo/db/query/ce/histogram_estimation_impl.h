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

#pragma once

#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/query/ce/histogram_common.h"
#include "mongo/db/query/stats/value_utils.h"

namespace mongo::ce {

enum class EstimationAlgo { HistogramV1, HistogramV2, HistogramV3 };

// --------------------- SCALAR HISTOGRAM ESTIMATION METHODS ---------------------

/**
 * Computes an estimate for a value and estimation type. Uses linear interpolation to
 * calculate the frequency of a value in a bucket.
 */
EstimationResult estimateCardinality(const stats::ScalarHistogram& h,
                                     sbe::value::TypeTags tag,
                                     sbe::value::Value val,
                                     EstimationType type);

/**
 * Computes an estimate for a range (low, high) and estimation type. Uses linear
 * interpolation to estimate the parts of buckets that fall in the range.
 */
EstimationResult estimateCardinalityRange(const stats::ScalarHistogram& histogram,
                                          bool lowInclusive,
                                          sbe::value::TypeTags tagLow,
                                          sbe::value::Value valLow,
                                          bool highInclusive,
                                          sbe::value::TypeTags tagHigh,
                                          sbe::value::Value valHigh);

/**
 * Returns cumulative total statistics for a histogram.
 */
EstimationResult getTotals(const stats::ScalarHistogram& h);

/**
 * Uses linear interpolation to estimate the cardinality and number of distinct
 * values (NDV) for a value that falls inside of a histogram bucket.
 */
EstimationResult interpolateEstimateInBucket(const stats::ScalarHistogram& h,
                                             sbe::value::TypeTags tag,
                                             sbe::value::Value val,
                                             EstimationType type,
                                             size_t bucketIndex);

/**
 * Computes an estimate for range query on array data with formula:
 * Card(ArrayMin(a < valHigh)) - Card(ArrayMax(a < valLow))
 */
EstimationResult estimateRangeQueryOnArray(const stats::ScalarHistogram& histogramAmin,
                                           const stats::ScalarHistogram& histogramAmax,
                                           bool lowInclusive,
                                           sbe::value::TypeTags tagLow,
                                           sbe::value::Value valLow,
                                           bool highInclusive,
                                           sbe::value::TypeTags tagHigh,
                                           sbe::value::Value valHigh);

// --------------------- CE HISTOGRAM ESTIMATION METHODS ---------------------

/**
 * Estimates the cardinality of an equality predicate given an CEHistogram and an SBE value and
 * type tag pair.
 */
EstimationResult estimateCardinalityEq(const stats::CEHistogram& ceHist,
                                       sbe::value::TypeTags tag,
                                       sbe::value::Value val,
                                       bool includeScalar);

/**
 * Estimates the cardinality of a range predicate given an CEHistogram and a range predicate.
 * Set 'includeScalar' to true to indicate whether or not the provided range should include no-array
 * values. The other fields define the range of the estimation.
 */
EstimationResult estimateCardinalityRange(const stats::CEHistogram& ceHist,
                                          bool lowInclusive,
                                          sbe::value::TypeTags tagLow,
                                          sbe::value::Value valLow,
                                          bool highInclusive,
                                          sbe::value::TypeTags tagHigh,
                                          sbe::value::Value valHigh,
                                          bool includeScalar,
                                          EstimationAlgo estAlgo = EstimationAlgo::HistogramV2);

/**
 * Estimates the selectivity of a given interval if histogram estimation is possible. Otherwise,
 * throw an exception.
 */
Cardinality estimateIntervalCardinality(const stats::CEHistogram& ceHist,
                                        const mongo::Interval& interval,
                                        bool includeScalar = true);

/**
 * Checks if a given bound can be estimated via either histograms or type counts.
 */
bool canEstimateBound(const stats::CEHistogram& ceHist,
                      sbe::value::TypeTags tag,
                      bool includeScalar);

}  // namespace mongo::ce
