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

#include "mongo/bson/bsontypes.h"
#include "mongo/bson/bsontypes_util.h"
#include "mongo/bson/json.h"
#include "mongo/db/catalog/create_collection.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/timeseries/bucket_catalog/execution_stats.h"
#include "mongo/db/timeseries/timeseries_test_fixture.h"
#include "mongo/db/timeseries/timeseries_write_util.h"
#include "mongo/db/timeseries/write_ops/internal/timeseries_write_ops_internal.h"
#include "mongo/db/timeseries/write_ops/timeseries_write_ops.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace mongo::timeseries::write_ops::internal {
namespace {
class TimeseriesWriteOpsInternalTest : public TimeseriesTestFixture {
protected:
    boost::optional<bucket_catalog::BucketMetadata> getBucketMetadata(BSONObj measurement) const;

    void _testBuildBatchedInsertContextWithMetaField(
        std::vector<BSONObj>& userMeasurementsBatch,
        stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>&
            metaFieldMetadataToCorrectIndexOrderMap,
        stdx::unordered_set<size_t>& expectedIndicesWithErrors) const;

    void _testBuildBatchedInsertContextOneBatchWithSameMetaFieldType(BSONType type) const;

    template <typename T>
    void _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        BSONType type = String,
        std::vector<T> metaValues = {_metaValue, _metaValue2, _metaValue3}) const;

    void _testBuildBatchedInsertContextWithoutMetaField(
        std::vector<BSONObj>& userMeasurementsBatch,
        std::vector<size_t>& correctIndexOrder,
        stdx::unordered_set<size_t>& expectedIndicesWithErrors) const;

    void _testStageInsertBatch(const NamespaceString& ns,
                               const UUID& collectionUUID,
                               std::vector<BSONObj> batchOfMeasurements,
                               std::vector<size_t> numWriteBatches) const;

    // Strings used to simulate kSize/kCachePressure rollover reason.
    std::string _bigStr = std::string(1000, 'a');

    // Should not be called
    CompressAndWriteBucketFunc _compressBucket = nullptr;
};

boost::optional<bucket_catalog::BucketMetadata> TimeseriesWriteOpsInternalTest::getBucketMetadata(
    BSONObj measurement) const {
    auto tsOptions = _getTimeseriesOptions(_ns1);
    auto metaValue = measurement.getField(_metaField);
    if (metaValue.eoo())
        return boost::none;
    return bucket_catalog::BucketMetadata{
        getTrackingContext(_bucketCatalog->trackingContexts,
                           bucket_catalog::TrackingScope::kMeasurementBatching),
        metaValue,
        tsOptions.getMetaField().get()};
}

void TimeseriesWriteOpsInternalTest::_testBuildBatchedInsertContextWithMetaField(
    std::vector<BSONObj>& userMeasurementsBatch,
    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>&
        metaFieldMetadataToCorrectIndexOrderMap,
    stdx::unordered_set<size_t>& expectedIndicesWithErrors) const {
    AutoGetCollection bucketsColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), LockMode::MODE_IX);
    tracking::Context trackingContext;
    timeseries::bucket_catalog::ExecutionStatsController stats;
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;
    auto tsOptions = _getTimeseriesOptions(_ns1);

    auto batchedInsertContextVector = buildBatchedInsertContextsWithMetaField(*_bucketCatalog,
                                                                              bucketsColl->uuid(),
                                                                              tsOptions,
                                                                              userMeasurementsBatch,
                                                                              stats,
                                                                              trackingContext,
                                                                              errorsAndIndices);

    ASSERT_EQ(batchedInsertContextVector.size(), metaFieldMetadataToCorrectIndexOrderMap.size());

    // Check that all of the tuples in each BatchedInsertContext have the correct order and
    // measurement.
    for (size_t i = 0; i < batchedInsertContextVector.size(); i++) {
        auto insertBatchContext = batchedInsertContextVector[i];
        ASSERT_EQ(insertBatchContext.key.metadata.getMetaField().get(), _metaField);
        auto metaFieldMetadata = insertBatchContext.key.metadata;
        ASSERT_EQ(insertBatchContext.measurementsTimesAndIndices.size(),
                  metaFieldMetadataToCorrectIndexOrderMap[metaFieldMetadata].size());

        for (size_t j = 0; j < insertBatchContext.measurementsTimesAndIndices.size(); j++) {
            auto tuple = insertBatchContext.measurementsTimesAndIndices[j];
            auto measurement = std::get<BSONObj>(tuple);
            bucket_catalog::BucketMetadata metadata = bucket_catalog::BucketMetadata{
                trackingContext, measurement[_metaField], tsOptions.getMetaField().get()};
            ASSERT(metadata == metaFieldMetadata);
            auto index = std::get<size_t>(tuple);
            ASSERT_EQ(index, metaFieldMetadataToCorrectIndexOrderMap[metaFieldMetadata][j]);
            ASSERT_EQ(userMeasurementsBatch[index].woCompare(measurement), 0);
        }
    }

    ASSERT_EQ(errorsAndIndices.size(), expectedIndicesWithErrors.size());

    // If we expected to see errors, check that the Statuses have the correct error code and
    // that there is a one-to-one mapping between indices that we expected to see errors for and
    // the indices that we did see errors for.
    for (size_t i = 0; i < errorsAndIndices.size(); i++) {
        auto writeStageErrorAndIndex = errorsAndIndices[i];
        ASSERT_EQ(writeStageErrorAndIndex.error.code(), ErrorCodes::BadValue);
        auto index = writeStageErrorAndIndex.index;
        ASSERT(expectedIndicesWithErrors.contains(index));
        expectedIndicesWithErrors.erase(index);
    }
    ASSERT(expectedIndicesWithErrors.empty());
};

void TimeseriesWriteOpsInternalTest::_testBuildBatchedInsertContextOneBatchWithSameMetaFieldType(
    BSONType type) const {
    std::vector<BSONObj> userMeasurementsBatch{
        _generateMeasurementWithMetaFieldType(type, Date_t::fromMillisSinceEpoch(200)),
        _generateMeasurementWithMetaFieldType(type, Date_t::fromMillisSinceEpoch(100)),
        _generateMeasurementWithMetaFieldType(type, Date_t::fromMillisSinceEpoch(101)),
        _generateMeasurementWithMetaFieldType(type, Date_t::fromMillisSinceEpoch(202)),
        _generateMeasurementWithMetaFieldType(type, Date_t::fromMillisSinceEpoch(201)),
    };

    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>
        metaFieldMetadataToCorrectIndexOrderMap;
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[0])),
        std::initializer_list<size_t>{1, 2, 0, 4, 3});
    stdx::unordered_set<size_t> expectedIndicesWithErrors;
    _testBuildBatchedInsertContextWithMetaField(
        userMeasurementsBatch, metaFieldMetadataToCorrectIndexOrderMap, expectedIndicesWithErrors);
}

template <typename T>
void TimeseriesWriteOpsInternalTest::
    _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        BSONType type, std::vector<T> metaValues) const {
    std::vector<BSONObj> userMeasurementsBatch{
        _generateMeasurementWithMetaFieldType(
            type, Date_t::fromMillisSinceEpoch(101), metaValues[1]),
        _generateMeasurementWithMetaFieldType(
            type, Date_t::fromMillisSinceEpoch(104), metaValues[2]),
        _generateMeasurementWithMetaFieldType(
            type, Date_t::fromMillisSinceEpoch(105), metaValues[0]),
        _generateMeasurementWithMetaFieldType(
            type, Date_t::fromMillisSinceEpoch(107), metaValues[0]),
        _generateMeasurementWithMetaFieldType(
            type, Date_t::fromMillisSinceEpoch(103), metaValues[1]),
        _generateMeasurementWithMetaFieldType(
            type, Date_t::fromMillisSinceEpoch(102), metaValues[2]),
        _generateMeasurementWithMetaFieldType(
            type, Date_t::fromMillisSinceEpoch(106), metaValues[0]),
    };
    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>
        metaFieldMetadataToCorrectIndexOrderMap;
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[2])), /* for _metaValues[0] */
        std::initializer_list<size_t>{2, 6, 3});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[0])), /* for _metaValue[1] */
        std::initializer_list<size_t>{0, 4});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[1])), /* for _metaValue[2] */
        std::initializer_list<size_t>{5, 1});
    stdx::unordered_set<size_t> expectedIndicesWithErrors;
    _testBuildBatchedInsertContextWithMetaField(
        userMeasurementsBatch, metaFieldMetadataToCorrectIndexOrderMap, expectedIndicesWithErrors);
}

void TimeseriesWriteOpsInternalTest::_testBuildBatchedInsertContextWithoutMetaField(
    std::vector<BSONObj>& userMeasurementsBatch,
    std::vector<size_t>& correctIndexOrder,
    stdx::unordered_set<size_t>& expectedIndicesWithErrors) const {
    AutoGetCollection autoColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();
    tracking::Context trackingContext;
    timeseries::bucket_catalog::ExecutionStatsController stats;
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;

    auto batchedInsertContextVector =
        buildBatchedInsertContextsNoMetaField(*_bucketCatalog,
                                              bucketsColl->uuid(),
                                              bucketsColl->getTimeseriesOptions().get(),
                                              userMeasurementsBatch,
                                              stats,
                                              trackingContext,
                                              errorsAndIndices);

    // Since we are inserting measurements with metadata values, all of the measurements should
    // fit into one batch. The only exception here will be when all of the measurements are
    // malformed, in which case we should have an empty vector.
    if (expectedIndicesWithErrors.size() == userMeasurementsBatch.size()) {
        ASSERT_EQ(batchedInsertContextVector.size(), 0);
    } else {
        ASSERT_EQ(batchedInsertContextVector.size(), 1);
        auto batchedInsertContext = batchedInsertContextVector.front();
        ASSERT_EQ(batchedInsertContext.key.metadata.getMetaField(), boost::none);

        // Check that all of the tuples in the BatchedInsertContext have the correct order and
        // measurement.
        for (size_t i = 0; i < batchedInsertContext.measurementsTimesAndIndices.size(); i++) {
            auto tuple = batchedInsertContext.measurementsTimesAndIndices[i];
            ASSERT_EQ(correctIndexOrder[i], std::get<size_t>(tuple));
            ASSERT_EQ(
                userMeasurementsBatch[correctIndexOrder[i]].woCompare(std::get<BSONObj>(tuple)), 0);
        }
    }

    ASSERT_EQ(errorsAndIndices.size(), expectedIndicesWithErrors.size());

    // If we expected to see errors, check that the Statuses have the correct error code and
    // that there is a one-to-one mapping between indices that we expected to see errors for and
    // the indices that we did see errors for.
    for (size_t i = 0; i < errorsAndIndices.size(); i++) {
        auto writeStageErrorAndIndex = errorsAndIndices[i];
        ASSERT_EQ(writeStageErrorAndIndex.error.code(), ErrorCodes::BadValue);
        auto index = writeStageErrorAndIndex.index;
        ASSERT(expectedIndicesWithErrors.contains(index));
        expectedIndicesWithErrors.erase(index);
    }
    ASSERT(expectedIndicesWithErrors.empty());
};

// numWriteBatches is the size of the writeBatches that should be generated from the
// batchedInsertContext at the same index.
// numWriteBatches.size() == batchedInsertContexts.size()
// sum(numWriteBatches) == the total number of buckets that should be written to from the input
// batchOfMeasurements.
// Example with RolloverReason::kSchemaChange and two distinct meta fields:
// batchOfMeasurements = [
//                         {_timeField: Date_t::now(), _metaField: "a",  "deathGrips": "isOnline"},
//                         {_timeField: Date_t::now(), _metaField: "a",  "deathGrips": "isOffline"},
//                         {_timeField: Date_t::now() + Seconds(1), _metaField: "a",  "deathGrips":
//                         100},
//                         {_timeField: Date_t::now(), _metaField: "b",  "deathGrips": "isOffline"},
//                       ]
// The batchedInsertContexts = [
//                      {bucketKey: [...], stripeNumber: 0, options: [...], stats: [...],
//                          measurementsTimesAndIndices: [
//                              {_timeField: Date_t::now(), _metaField: "a",  "deathGrips":
//                              "isOnline"},
//                              {_timeField: Date_t::now(), _metaField: "a",  "deathGrips":
//                              "isOffline"},
//                              {_timeField: Date_t::now() + Seconds(1), _metaField: "a",
//                              "deathGrips": 100},
//                          ]},
//                       {bucketKey: [...], stripeNumber: 1, options: [...], stats: [...],
//                          measurementsTimesAndIndices: [
//                              {_timeField: Date_t::now(), _metaField: "b",  "deathGrips":
//                              "isOffline"},
//                          ]},
//                     ]
// Note: we simplified the measurementsTimesAndIndices vector to consist of measurement's BSONObj
// and not measurement's BatchedInsertTuple.
//
// numWriteBatches = [2, 1]
//
// We are accessing {_metaField: "a"}/batchedInsertContexts[0] and write the first two elements into
// one bucket.
// We detect a schema change with the "deathGrips" field for the last measurement in
// batchedInsertContexts[0].measurementsTimesAndIndices and write this measurement to a second
// bucket.
// This means for {_metaField: "a"}/batchedInsertContexts[0], we have two distinct write
// batches (numWriteBatches[0]).
//
// We then write one measurement to a third bucket because we have a distinct {_metaField:
// "b"} in batchedInsertContexts[1] (that doesn't hash to the same stripe).
// This means for {_metaField: "b"}/batchedInsertContexts[1], we have one distinct write batch
// (numWriteBatches[1]).

// If we are attempting to trigger kCachePressure, we must call this function with
// _storageCacheSizeBytes = kLimitedStorageCacheSizeBytes. Otherwise, _storageCacheSizeBytes
// = kDefaultStorageCacheSizeBytes.
void TimeseriesWriteOpsInternalTest::_testStageInsertBatch(
    const NamespaceString& ns,
    const UUID& collectionUUID,
    std::vector<BSONObj> batchOfMeasurements,
    std::vector<size_t> numWriteBatches) const {
    AutoGetCollection autoColl(_opCtx, ns.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();
    auto timeseriesOptions = _getTimeseriesOptions(ns);
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;

    auto batchedInsertContexts = write_ops::internal::buildBatchedInsertContexts(
        *_bucketCatalog, collectionUUID, timeseriesOptions, batchOfMeasurements, errorsAndIndices);
    ASSERT(errorsAndIndices.empty());

    ASSERT_EQ(batchedInsertContexts.size(), numWriteBatches.size());
    size_t numMeasurements = 0;
    for (size_t i = 0; i < batchedInsertContexts.size(); i++) {
        numMeasurements += batchedInsertContexts[i].measurementsTimesAndIndices.size();
    }
    ASSERT_EQ(numMeasurements, batchOfMeasurements.size());

    for (size_t i = 0; i < batchedInsertContexts.size(); i++) {
        auto writeBatches =
            write_ops::internal::stageInsertBatch(_opCtx,
                                                  *_bucketCatalog,
                                                  bucketsColl.get(),
                                                  _opCtx->getOpID(),
                                                  nullptr /*comparator*/,
                                                  _storageCacheSizeBytes,
                                                  nullptr /*compressAndWriteBucketFunc*/,
                                                  batchedInsertContexts[i]);
        ASSERT_EQ(writeBatches.size(), numWriteBatches[i]);
    }
}

TEST_F(TimeseriesWriteOpsInternalTest, BuildBatchedInsertContextsOneBatchNoMetafield) {
    std::vector<BSONObj> userMeasurementsBatch{
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:05:00.000Z"}, "x":1})"),
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:04:00.000Z"}, "x":2})"),
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:02:00.000Z"}, "x":3})"),
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:03:00.000Z"}, "x":4})"),
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:01:00.000Z"}, "x":5})"),
    };
    std::vector<size_t> correctIndexOrder{4, 2, 3, 1, 0};
    stdx::unordered_set<size_t> expectedIndicesWithErrors;
    _testBuildBatchedInsertContextWithoutMetaField(
        userMeasurementsBatch, correctIndexOrder, expectedIndicesWithErrors);
}

TEST_F(TimeseriesWriteOpsInternalTest, BuildBatchedInsertContextsOneBatchWithMetafield) {
    // Test with all possible BSONTypes.
    std::vector<BSONType> allBSONTypes = _getFlattenedVector(std::vector<std::vector<BSONType>>{
        _stringComponentBSONTypes, _nonStringComponentVariableBSONTypes, _constantBSONTypes});
    for (BSONType type : allBSONTypes) {
        _testBuildBatchedInsertContextOneBatchWithSameMetaFieldType(type);
    }
}

TEST_F(TimeseriesWriteOpsInternalTest, BuildBatchedInsertContextsMultipleBatchesWithMetafield) {
    // Test with BSONTypes that aren't constant (so we create distinct batches).
    // BSONTypes that have a StringData component.
    for (BSONType type : _stringComponentBSONTypes) {
        _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
            type, std::vector<StringData>{_metaValue, _metaValue2, _metaValue3});
    }

    // Test with BSONTypes that don't have a StringData type metaValue.
    _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        bsonTimestamp, std::vector<Timestamp>{Timestamp(1, 2), Timestamp(2, 3), Timestamp(3, 4)});
    _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        NumberInt, std::vector<int>{365, 10, 4});
    _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        NumberLong,
        std::vector<long long>{0x0123456789aacdeff, 0x0fedcba987654321, 0x0123456789abcdefll});
    _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        NumberDecimal,
        std::vector<Decimal128>{Decimal128("0.490"), Decimal128("0.30"), Decimal128("1.50")});
    _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        NumberDouble, std::vector<double>{1.5, 1.4, 1.3});
    _testBuildBatchedInsertContextMultipleBatchesWithSameMetaFieldType(
        jstOID,
        std::vector<OID>{OID("00000000ff00000000000002"),
                         OID("000000000000000000000002"),
                         OID("0000000000fff00000000002")});
    // We don't include bool because it only has two distinct meta values when this test requires 3.
    // We don't include BinDataType because we don't have a constructor for
    // std::vector<BinDataType>.
    // We make up for this missing coverage for both BinDataType and Bool in the
    // BuildBatchedInsertContextsMultipleBatchesWithDifferentMetafieldTypes1/2 unit tests.
}

TEST_F(TimeseriesWriteOpsInternalTest,
       BuildBatchedInsertContextsMultipleBatchesWithDifferentMetafieldTypes1) {
    std::vector<BSONObj> userMeasurementsBatch{
        _generateMeasurementWithMetaFieldType(jstOID, Date_t::fromMillisSinceEpoch(113)),
        _generateMeasurementWithMetaFieldType(Code, Date_t::fromMillisSinceEpoch(105), _metaValue),
        _generateMeasurementWithMetaFieldType(Code, Date_t::fromMillisSinceEpoch(107), _metaValue),
        _generateMeasurementWithMetaFieldType(
            DBRef, Date_t::fromMillisSinceEpoch(103), _metaValue2),
        _generateMeasurementWithMetaFieldType(jstOID, Date_t::fromMillisSinceEpoch(104)),
        _generateMeasurementWithMetaFieldType(
            String, Date_t::fromMillisSinceEpoch(102), _metaValue3),
        _generateMeasurementWithMetaFieldType(EOO, Date_t::fromMillisSinceEpoch(109)),
        _generateMeasurementWithMetaFieldType(
            String, Date_t::fromMillisSinceEpoch(108), _metaValue),
        _generateMeasurementWithMetaFieldType(
            String, Date_t::fromMillisSinceEpoch(111), _metaValue),
        _generateMeasurementWithMetaFieldType(BinData, Date_t::fromMillisSinceEpoch(204)),
        _generateMeasurementWithMetaFieldType(Code, Date_t::fromMillisSinceEpoch(200), _metaValue2),
        _generateMeasurementWithMetaFieldType(
            String, Date_t::fromMillisSinceEpoch(101), _metaValue3),
        _generateMeasurementWithMetaFieldType(
            String, Date_t::fromMillisSinceEpoch(121), _metaValue3),
        _generateMeasurementWithMetaFieldType(String, Date_t::fromMillisSinceEpoch(65), _metaValue),
        _generateMeasurementWithMetaFieldType(
            DBRef, Date_t::fromMillisSinceEpoch(400), _metaValue2),
        _generateMeasurementWithMetaFieldType(MinKey, Date_t::fromMillisSinceEpoch(250)),
        _generateMeasurementWithMetaFieldType(EOO, Date_t::fromMillisSinceEpoch(108)),
        _generateMeasurementWithMetaFieldType(MaxKey, Date_t::fromMillisSinceEpoch(231)),
        _generateMeasurementWithMetaFieldType(EOO, Date_t::fromMillisSinceEpoch(107)),
        _generateMeasurementWithMetaFieldType(
            BinData, Date_t::fromMillisSinceEpoch(204), BSONBinData("", 1, BinDataGeneral)),
    };
    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>
        metaFieldMetadataToCorrectIndexOrderMap;
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[0])), /* for jstOID */
        std::initializer_list<size_t>{4, 0});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[1])), /* for Code (_metaValue) */
        std::initializer_list<size_t>{1, 2});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[10])), /* for Code (_metaValue2) */
        std::initializer_list<size_t>{10});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[6])), /* for EOO */
        std::initializer_list<size_t>{18, 16, 6});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[3])), /* for DBRef (_metaValue2) */
        std::initializer_list<size_t>{3, 14});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[7])), /* for String (_metaValue) */
        std::initializer_list<size_t>{13, 7, 8});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[5])), /* for String (_metaValue3) */
        std::initializer_list<size_t>{11, 5, 12});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[9])), /* for BinData (0) */
        std::initializer_list<size_t>{9});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[19])), /* for BinData (1) */
        std::initializer_list<size_t>{19});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[15])), /* for MinKey */
        std::initializer_list<size_t>{15});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[17])), /* for MaxKey */
        std::initializer_list<size_t>{17});
    stdx::unordered_set<size_t> expectedIndicesWithErrors;
    _testBuildBatchedInsertContextWithMetaField(
        userMeasurementsBatch, metaFieldMetadataToCorrectIndexOrderMap, expectedIndicesWithErrors);
}

TEST_F(TimeseriesWriteOpsInternalTest,
       BuildBatchedInsertContextsMultipleBatchesWithDifferentMetafieldTypes2) {
    std::vector<BSONObj> userMeasurementsBatch{
        _generateMeasurementWithMetaFieldType(
            Symbol, Date_t::fromMillisSinceEpoch(382), _metaValue2),
        _generateMeasurementWithMetaFieldType(
            CodeWScope, Date_t::fromMillisSinceEpoch(493), _metaValue2),
        _generateMeasurementWithMetaFieldType(Object, Date_t::fromMillisSinceEpoch(212)),
        _generateMeasurementWithMetaFieldType(
            Symbol, Date_t::fromMillisSinceEpoch(284), _metaValue2),
        _generateMeasurementWithMetaFieldType(
            CodeWScope, Date_t::fromMillisSinceEpoch(958), _metaValue2),
        _generateMeasurementWithMetaFieldType(Object, Date_t::fromMillisSinceEpoch(103)),
        _generateMeasurementWithMetaFieldType(
            Object, Date_t::fromMillisSinceEpoch(492), _metaValue2),
        _generateMeasurementWithMetaFieldType(Object, Date_t::fromMillisSinceEpoch(365)),
        _generateMeasurementWithMetaFieldType(jstNULL, Date_t::fromMillisSinceEpoch(590)),
        _generateMeasurementWithMetaFieldType(Array, Date_t::fromMillisSinceEpoch(204)),
        _generateMeasurementWithMetaFieldType(jstNULL, Date_t::fromMillisSinceEpoch(58)),
        _generateMeasurementWithMetaFieldType(
            CodeWScope, Date_t::fromMillisSinceEpoch(93), _metaValue3),
        _generateMeasurementWithMetaFieldType(
            CodeWScope, Date_t::fromMillisSinceEpoch(304), _metaValue3),
        _generateMeasurementWithMetaFieldType(jstNULL, Date_t::fromMillisSinceEpoch(384)),
        _generateMeasurementWithMetaFieldType(CodeWScope, Date_t::fromMillisSinceEpoch(888)),
        _generateMeasurementWithMetaFieldType(Array, Date_t::fromMillisSinceEpoch(764)),
        _generateMeasurementWithMetaFieldType(Array, Date_t::fromMillisSinceEpoch(593)),
    };

    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>
        metaFieldMetadataToCorrectIndexOrderMap;
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[0])), /* for Symbol */
        std::initializer_list<size_t>{3, 0});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[1])), /* for CodeWScope (_metaValue2) */
        std::initializer_list<size_t>{1, 4});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[14])), /* for CodeWScope (_metaValue) */
        std::initializer_list<size_t>{14});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[11])), /* for CodeWScope (_metaValue3) */
        std::initializer_list<size_t>{11, 12});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[2])), /* for Object (_metaValue) */
        std::initializer_list<size_t>{5, 2, 7});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[6])), /* for Object (_metaValue2) */
        std::initializer_list<size_t>{6});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[8])), /* for jstNULL */
        std::initializer_list<size_t>{10, 13, 8});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[9])), /* for Array */
        std::initializer_list<size_t>{9, 16, 15});
    stdx::unordered_set<size_t> expectedIndicesWithErrors;
    _testBuildBatchedInsertContextWithMetaField(
        userMeasurementsBatch, metaFieldMetadataToCorrectIndexOrderMap, expectedIndicesWithErrors);
}

TEST_F(TimeseriesWriteOpsInternalTest,
       BuildBatchedInsertContextsMultipleBatchesWithDifferentMetafieldTypes3) {
    std::vector<BSONObj> userMeasurementsBatch{
        _generateMeasurementWithMetaFieldType(Date, Date_t::fromMillisSinceEpoch(113)),
        _generateMeasurementWithMetaFieldType(RegEx, Date_t::fromMillisSinceEpoch(105)),
        _generateMeasurementWithMetaFieldType(RegEx, Date_t::fromMillisSinceEpoch(107)),
        _generateMeasurementWithMetaFieldType(Undefined, Date_t::fromMillisSinceEpoch(103)),
        _generateMeasurementWithMetaFieldType(Bool, Date_t::fromMillisSinceEpoch(104), true),
        _generateMeasurementWithMetaFieldType(Bool, Date_t::fromMillisSinceEpoch(102), true),
        _generateMeasurementWithMetaFieldType(Bool, Date_t::fromMillisSinceEpoch(104), false),
        _generateMeasurementWithMetaFieldType(Bool, Date_t::fromMillisSinceEpoch(102), false),
        _generateMeasurementWithMetaFieldType(Undefined, Date_t::fromMillisSinceEpoch(102)),
        _generateMeasurementWithMetaFieldType(EOO, Date_t::fromMillisSinceEpoch(109)),
        _generateMeasurementWithMetaFieldType(NumberInt, Date_t::fromMillisSinceEpoch(108)),
        _generateMeasurementWithMetaFieldType(NumberInt, Date_t::fromMillisSinceEpoch(111)),
        _generateMeasurementWithMetaFieldType(NumberDouble, Date_t::fromMillisSinceEpoch(204), 2.3),
        _generateMeasurementWithMetaFieldType(NumberLong, Date_t::fromMillisSinceEpoch(200)),
        _generateMeasurementWithMetaFieldType(NumberDouble, Date_t::fromMillisSinceEpoch(101), 2.1),
        _generateMeasurementWithMetaFieldType(NumberDouble, Date_t::fromMillisSinceEpoch(121), 2.3),
        _generateMeasurementWithMetaFieldType(Date, Date_t::fromMillisSinceEpoch(65)),
        _generateMeasurementWithMetaFieldType(
            NumberDecimal, Date_t::fromMillisSinceEpoch(400), Decimal128("0.4")),
        _generateMeasurementWithMetaFieldType(
            NumberDecimal, Date_t::fromMillisSinceEpoch(400), Decimal128("0.3")),
        _generateMeasurementWithMetaFieldType(RegEx, Date_t::fromMillisSinceEpoch(108)),
        _generateMeasurementWithMetaFieldType(bsonTimestamp, Date_t::fromMillisSinceEpoch(231)),
        _generateMeasurementWithMetaFieldType(EOO, Date_t::fromMillisSinceEpoch(107)),
    };
    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>
        metaFieldMetadataToCorrectIndexOrderMap;
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[0])), /* for Date */
        std::initializer_list<size_t>{16, 0});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[1])), /* for RegEx */
        std::initializer_list<size_t>{1, 2, 19});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[3])), /* for Undefined  */
        std::initializer_list<size_t>{8, 3});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[4])), /* for Bool (true) */
        std::initializer_list<size_t>{5, 4});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[6])), /* for Bool (false) */
        std::initializer_list<size_t>{7, 6});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[9])), /* for EOO  */
        std::initializer_list<size_t>{21, 9});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[10])), /* for NumberInt */
        std::initializer_list<size_t>{10, 11});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[12])), /* for NumberDouble (2.3) */
        std::initializer_list<size_t>{15, 12});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[13])), /* for NumberLong */
        std::initializer_list<size_t>{13});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[14])), /* for NumberDouble (2.1) */
        std::initializer_list<size_t>{14});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[17])), /* for NumberDecimal (0.4) */
        std::initializer_list<size_t>{17});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[18])), /* for NumberDecimal (0.3) */
        std::initializer_list<size_t>{18});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[20])), /* for bsonTimestamp */
        std::initializer_list<size_t>{20});
    stdx::unordered_set<size_t> expectedIndicesWithErrors;
    _testBuildBatchedInsertContextWithMetaField(
        userMeasurementsBatch, metaFieldMetadataToCorrectIndexOrderMap, expectedIndicesWithErrors);
}

TEST_F(TimeseriesWriteOpsInternalTest,
       BuildBatchedInsertContextsNoMetaReportsMalformedMeasurements) {
    std::vector<BSONObj> userMeasurementsBatch{
        mongo::fromjson(R"({"x":1})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:04:00.000Z"}, "x":2})"),
        mongo::fromjson(R"({"x":3})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:03:00.000Z"}, "x":4})"),
        mongo::fromjson(R"({"x":5})"),  // Malformed measurement, missing time field
    };
    std::vector<size_t> correctIndexOrder{3, 1};
    stdx::unordered_set<size_t> expectedIndicesWithErrors{0, 2, 4};
    _testBuildBatchedInsertContextWithoutMetaField(
        userMeasurementsBatch, correctIndexOrder, expectedIndicesWithErrors);
}

TEST_F(TimeseriesWriteOpsInternalTest,
       BuildBatchedInsertContextsWithMetaReportsMalformedMeasurements) {
    std::vector<BSONObj> userMeasurementsBatch{
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:05:00.000Z"}, "tag":"a", "x":1})"),
        mongo::fromjson(R"({"tag":"a", "x":2})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"x":3,"tag":"a"})"),   // Malformed measurement, missing time field
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:03:00.000Z"}, "tag":"b", "x":4})"),
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:01:00.000Z"}, "tag":"a", "x":5})"),
    };
    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>
        metaFieldMetadataToCorrectIndexOrderMap;
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[0])), /* for metaValue: "a" */
        std::initializer_list<size_t>{4, 0});
    metaFieldMetadataToCorrectIndexOrderMap.try_emplace(
        *(getBucketMetadata(userMeasurementsBatch[3])), /* for metaValue: "b" */
        std::initializer_list<size_t>{3});
    stdx::unordered_set<size_t> expectedIndicesWithErrors{1, 2};
    _testBuildBatchedInsertContextWithMetaField(
        userMeasurementsBatch, metaFieldMetadataToCorrectIndexOrderMap, expectedIndicesWithErrors);
}

TEST_F(TimeseriesWriteOpsInternalTest, BuildBatchedInsertContextsAllMeasurementsErrorNoMeta) {
    std::vector<BSONObj> userMeasurementsBatch{
        mongo::fromjson(R"({"x":2})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"x":3})"),  // Malformed measurement, missing time field
    };
    std::vector<size_t> correctIndexOrder;
    stdx::unordered_set<size_t> expectedIndicesWithErrors{0, 1};
    _testBuildBatchedInsertContextWithoutMetaField(
        userMeasurementsBatch, correctIndexOrder, expectedIndicesWithErrors);
};

TEST_F(TimeseriesWriteOpsInternalTest, BuildBatchedInsertContextsAllMeasurementsErrorWithMeta) {
    std::vector<BSONObj> userMeasurementsBatch{
        mongo::fromjson(R"({"tag":"a", "x":2})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"tag":"a", "x":3})"),  // Malformed measurement, missing time field
    };
    stdx::unordered_map<bucket_catalog::BucketMetadata, std::vector<size_t>>
        metaFieldMetadataToCorrectIndexOrderMap;
    stdx::unordered_set<size_t> expectedIndicesWithErrors{0, 1};
    _testBuildBatchedInsertContextWithMetaField(
        userMeasurementsBatch, metaFieldMetadataToCorrectIndexOrderMap, expectedIndicesWithErrors);
};

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchFillsUpSingleBucket) {
    std::vector<BSONObj> batchOfMeasurementsTimeseriesBucketMaxCount =
        _generateMeasurementsWithRolloverReason({.reason = bucket_catalog::RolloverReason::kNone});
    std::vector<size_t> numWriteBatches{1};
    _testStageInsertBatch(
        _ns1, _uuid1, batchOfMeasurementsTimeseriesBucketMaxCount, numWriteBatches);

    // Testing the same configuration as above, but in a collection without a meta field.
    std::vector<BSONObj> batchOfMeasurementsTimeseriesBucketMaxCountNoMetaField =
        _generateMeasurementsWithRolloverReason(
            {.reason = bucket_catalog::RolloverReason::kNone, .metaValue = boost::none});
    _testStageInsertBatch(_nsNoMeta,
                          _uuidNoMeta,
                          batchOfMeasurementsTimeseriesBucketMaxCountNoMetaField,
                          numWriteBatches);
}

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchHandlesRolloverReasonkCount) {
    auto batchOfMeasurementsWithCount =
        _generateMeasurementsWithRolloverReason({.reason = bucket_catalog::RolloverReason::kCount});

    // batchOfMeasurements will be 2 * gTimeseriesBucketMaxCount measurements with all the
    // measurements having the same meta field and time, which means we should have two buckets.
    std::vector<size_t> numWriteBatches{2};
    _testStageInsertBatch(_ns1, _uuid1, batchOfMeasurementsWithCount, numWriteBatches);

    // Testing the same configuration as above, but in a collection without a meta field.
    auto batchOfMeasurementsWithCountNoMetaField = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kCount, .metaValue = boost::none});
    _testStageInsertBatch(
        _nsNoMeta, _uuidNoMeta, batchOfMeasurementsWithCountNoMetaField, numWriteBatches);
}

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchHandlesRolloverReasonkTimeForward) {
    // Max bucket size with only the last measurement having kTimeForward.
    auto batchOfMeasurementsWithTimeForwardAtEnd = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kTimeForward, .metaValue = _metaValue});

    // The last measurement in batchOfMeasurementsWithTimeForwardAtEnd will have a timestamp outside
    // of the bucket range encompassing the previous measurements, which means that this
    // measurement will be in a different bucket.
    std::vector<size_t> numWriteBatches{2};
    _testStageInsertBatch(_ns1, _uuid1, batchOfMeasurementsWithTimeForwardAtEnd, numWriteBatches);

    // Max bucket size with measurements[1:gTimeseriesBucketMaxCount] having kTimeForward.
    // We declare a different meta field so we don't attempt to use the buckets created above.
    auto batchOfMeasurementsWithTimeForwardAfterFirstMeasurementNoMetaField =
        _generateMeasurementsWithRolloverReason(
            {.reason = bucket_catalog::RolloverReason::kTimeForward,
             .idxWithDiffMeasurement = 1,
             .metaValue = boost::none});
    _testStageInsertBatch(_nsNoMeta,
                          _uuidNoMeta,
                          batchOfMeasurementsWithTimeForwardAfterFirstMeasurementNoMetaField,
                          numWriteBatches);

    // 50 measurements with measurements[25:50] having kTimeForward.
    // We declare a different meta field so we don't attempt to use the buckets created above.
    auto batchOfMeasurementsWithTimeForwardInMiddle = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kTimeForward,
         .numMeasurements = 50,
         .idxWithDiffMeasurement = 25,
         .metaValue = _metaValue2});
    _testStageInsertBatch(
        _ns2, _uuid2, batchOfMeasurementsWithTimeForwardInMiddle, numWriteBatches);
}

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchHandlesRolloverReasonkSchemaChange) {
    // Max bucket size with only the last measurement having kSchemaChange.
    auto batchOfMeasurementsWithSchemaChangeAtEnd = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kSchemaChange, .metaValue = _metaValue});

    // The last measurement in batchOfMeasurementsWithSchemaChangeAtEnd will have a int rather than
    // a string for field "deathGrips", which means this measurement will be in a different
    // bucket.
    std::vector<size_t> numWriteBatches{2};
    _testStageInsertBatch(_ns1, _uuid1, batchOfMeasurementsWithSchemaChangeAtEnd, numWriteBatches);

    // Max bucket size with measurements[1:gTimeseriesBucketMaxCount] having kSchemaChange.
    // We declare a different meta field so we don't attempt to use a bucket created above.
    auto batchOfMeasurementsWithSchemaChangeAfterFirstMeasurementNoMetaField =
        _generateMeasurementsWithRolloverReason(
            {.reason = bucket_catalog::RolloverReason::kSchemaChange,
             .idxWithDiffMeasurement = 1,
             .metaValue = boost::none});
    _testStageInsertBatch(_nsNoMeta,
                          _uuidNoMeta,
                          batchOfMeasurementsWithSchemaChangeAfterFirstMeasurementNoMetaField,
                          numWriteBatches);

    // 50 measurements with measurements[25:50] having kSchemaChange.
    // We declare a different meta field so we don't attempt to use a bucket created above.
    auto batchOfMeasurementsWithTimeForwardInMiddle = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kSchemaChange,
         .numMeasurements = 50,
         .idxWithDiffMeasurement = 25,
         .metaValue = _metaValue2});

    _testStageInsertBatch(
        _ns2, _uuid2, batchOfMeasurementsWithTimeForwardInMiddle, numWriteBatches);
}

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchHandlesRolloverReasonkSize) {
    auto batchOfMeasurementsWithSize =
        _generateMeasurementsWithRolloverReason({.reason = bucket_catalog::RolloverReason::kSize});

    // The last measurement will exceed the size that the bucket can store, which will mean it
    // should be in a different bucket.
    std::vector<size_t> numWriteBatches{2};
    _testStageInsertBatch(_ns1, _uuid1, batchOfMeasurementsWithSize, numWriteBatches);

    // Testing the same configuration as above, but in a collection without a meta field.
    auto batchOfMeasurementsWithSizeNoMetaField = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kSize, .metaValue = boost::none});
    _testStageInsertBatch(
        _nsNoMeta, _uuidNoMeta, batchOfMeasurementsWithSizeNoMetaField, numWriteBatches);
}

TEST_F(TimeseriesWriteOpsInternalTest, InsertBatchHandlesRolloverReasonkCachePressure) {
    // Artificially lower _storageCacheSizeBytes so we can simulate kCachePressure.
    _storageCacheSizeBytes = kLimitedStorageCacheSizeBytes;

    auto batchOfMeasurementsWithCachePressure = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kCachePressure});

    // The last measurement will exceed the size that the bucket can store. Coupled with the lowered
    // cache size, we will trigger kCachePressure, so the measurement will be in a different bucket.
    std::vector<size_t> numWriteBatches{2};
    _testStageInsertBatch(_ns1, _uuid1, batchOfMeasurementsWithCachePressure, numWriteBatches);

    // Testing the same configuration as above, but in a collection without a meta field.
    auto batchOfMeasurementsWithCachePressureNoMetaField = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kCachePressure, .metaValue = boost::none});
    _testStageInsertBatch(
        _nsNoMeta, _uuidNoMeta, batchOfMeasurementsWithCachePressureNoMetaField, numWriteBatches);

    // Reset _storageCacheSizeBytes back to a representative value.
    _storageCacheSizeBytes = kDefaultStorageCacheSizeBytes;
}

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchHandlesRolloverReasonMixed1) {
    std::vector<BSONObj> batchOfMeasurements =
        _generateMeasurementsWithRolloverReason({.reason = bucket_catalog::RolloverReason::kNone,
                                                 .numMeasurements = 50,
                                                 .timeValue = Date_t::now()});

    auto batchOfMeasurementsWithCount =
        _generateMeasurementsWithRolloverReason({.reason = bucket_catalog::RolloverReason::kCount,
                                                 .timeValue = Date_t::now() + Seconds(1)});

    auto batchOfMeasurementsWithTimeForward = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kTimeForward,
         .numMeasurements = 50,
         .idxWithDiffMeasurement = 25,
         .timeValue = Date_t::now() + Seconds(2)});


    std::vector<BSONObj> mixedRolloverReasonsMeasurements = _getFlattenedVector(
        std::vector<std::vector<BSONObj>>({batchOfMeasurements,
                                           batchOfMeasurementsWithCount,
                                           batchOfMeasurementsWithTimeForward}));
    ASSERT_EQ(mixedRolloverReasonsMeasurements.size(),
              batchOfMeasurements.size() + batchOfMeasurementsWithCount.size() +
                  batchOfMeasurementsWithTimeForward.size());

    // The first bucket will consist of all 50 measurements from batchOfMeasurements.
    // We will then insert gTimeseriesBucketMaxCount - 50 measurements from
    // batchOfMeasurementsWithCountA before rolling over due to kCount.
    // We will create a second bucket that will have the gTimeseriesBucketMaxCount measurements
    // from batchOfMeasurementsWithCount, and then roll over due to kCount.
    // We will then create a third bucket that will have the last 50 measurements from
    // batchOfMeasurementsWithCount.
    // We will then insert 25 measurements from batchOfMeasurementsWithTimeForward into the third
    // bucket before encountering kTimeForward and add the remaining 25 measurements into a fourth
    // bucket.
    std::vector<size_t> numWriteBatches{4};
    _testStageInsertBatch(_ns1, _uuid1, mixedRolloverReasonsMeasurements, numWriteBatches);
}

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchHandlesRolloverReasonMixed2) {
    auto batchOfMeasurementsWithSize = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kSize, .timeValue = Date_t::now()});

    std::vector<BSONObj> batchOfMeasurements =
        _generateMeasurementsWithRolloverReason({.reason = bucket_catalog::RolloverReason::kNone,
                                                 .numMeasurements = 50,
                                                 .timeValue = Date_t::now() + Seconds(1)});

    auto batchOfMeasurementsWithSchemaChange = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kSchemaChange,
         .timeValue = Date_t::now() + Seconds(2)});

    std::vector<BSONObj> mixedRolloverReasonsMeasurements =
        _getFlattenedVector(std::vector<std::vector<BSONObj>>{
            batchOfMeasurementsWithSize, batchOfMeasurements, batchOfMeasurementsWithSchemaChange});
    ASSERT_EQ(mixedRolloverReasonsMeasurements.size(),
              batchOfMeasurementsWithSize.size() + batchOfMeasurements.size() +
                  batchOfMeasurementsWithSchemaChange.size());

    // The first bucket will consist of 124 measurements before we rollover due to kSize from
    // batchOfMeasurementsWithSize.
    // The second bucket will consist of one measurement from batchOfMeasurementsWithSize with
    // the 50 measurements from batchOfMeasurements. We will then insert
    // gTimeseriesBucketMaxCount - 50 - 1 measurements from batchOfMeasurementsWithSchemaChange
    // into this second bucket, and rollover due to kCount. The third bucket will be created and
    // we will insert the measurements
    // batchOfMeasurementsWithSchemaChange[gTimeseriesBucketMaxCount - 50 -
    // 1:gTimeseriesBucketMaxCount-1].
    // Finally, the fourth bucket created to insert the last measurement
    // batchOfMeasurementsWithSchemaChange due to kSchemaChange.
    std::vector<size_t> numWriteBatches{4};
    _testStageInsertBatch(_ns1, _uuid1, mixedRolloverReasonsMeasurements, numWriteBatches);
}

TEST_F(TimeseriesWriteOpsInternalTest, StageInsertBatchHandlesRolloverReasonMixed3) {
    // Artificially lower _storageCacheSizeBytes so we can simulate kCachePressure.
    _storageCacheSizeBytes = kLimitedStorageCacheSizeBytes;

    auto batchOfMeasurements =
        _generateMeasurementsWithRolloverReason({.reason = bucket_catalog::RolloverReason::kNone,
                                                 .numMeasurements = 10,
                                                 .timeValue = Date_t::now()});

    std::vector<BSONObj> batchOfMeasurementsWithTimeForward =
        _generateMeasurementsWithRolloverReason(
            {.reason = bucket_catalog::RolloverReason::kTimeForward,
             .numMeasurements = 10,
             .idxWithDiffMeasurement = 5,
             .timeValue = Date_t::now() + Seconds(1)});

    auto batchOfMeasurementsWithCachePressure = _generateMeasurementsWithRolloverReason(
        {.reason = bucket_catalog::RolloverReason::kCachePressure,
         .timeValue = Date_t::now() + Seconds(2)});

    std::vector<BSONObj> mixedRolloverReasonsMeasurements = _getFlattenedVector(
        std::vector<std::vector<BSONObj>>{batchOfMeasurements,
                                          batchOfMeasurementsWithTimeForward,
                                          batchOfMeasurementsWithCachePressure});
    ASSERT_EQ(mixedRolloverReasonsMeasurements.size(),
              batchOfMeasurements.size() + batchOfMeasurementsWithTimeForward.size() +
                  batchOfMeasurementsWithCachePressure.size());

    // The first bucket will consist of 10 measurements from batchOfMeasurements. We will then
    // insert 5 measurements from batchOfMeasurementsWithTimeForward until we rollover due to
    // kTimeForward.
    // The second bucket will consist of the remaining 5 measurements from
    // batchOfMeasurementsWithTimeForward. We will then insert 4 measurements from
    // batchOfMeasurementsWithCachePressure into the second bucket and will rollover due to
    // kCachePressure.
    // We will insert the last measurement from batchOfMeasurementsWithCachePressure into a
    // third bucket.
    std::vector<size_t> numWriteBatches{3};
    _testStageInsertBatch(_ns1, _uuid1, mixedRolloverReasonsMeasurements, numWriteBatches);

    // Reset _storageCacheSizeBytes back to a representative value.
    _storageCacheSizeBytes = kDefaultStorageCacheSizeBytes;
}

TEST_F(TimeseriesWriteOpsInternalTest, PrepareInsertsToBucketsSimpleOneFullBucket) {
    auto tsOptions = _getTimeseriesOptions(_ns1);
    std::vector<BSONObj> userBatch;
    for (auto i = 0; i < gTimeseriesBucketMaxCount; i++) {
        userBatch.emplace_back(BSON(_timeField << Date_t::now() << _metaField << _metaValue));
    }

    std::vector<WriteStageErrorAndIndex> errorsAndIndices;

    AutoGetCollection autoColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();

    auto swWriteBatches = prepareInsertsToBuckets(_opCtx,
                                                  *_bucketCatalog,
                                                  bucketsColl.get(),
                                                  tsOptions,
                                                  _opCtx->getOpID(),
                                                  _getCollator(_ns1),
                                                  _getStorageCacheSizeBytes(),
                                                  /*earlyReturnOnError=*/true,
                                                  _compressBucket,
                                                  userBatch,
                                                  errorsAndIndices);

    ASSERT_TRUE(swWriteBatches.isOK());
    ASSERT_TRUE(errorsAndIndices.empty());

    auto& writeBatches = swWriteBatches.getValue();
    ASSERT_EQ(writeBatches.size(), 1);

    for (size_t i = 0; i < writeBatches.size(); i++) {
        ASSERT_EQ(writeBatches[i]->isReopened, false);
        ASSERT_EQ(writeBatches[i]->bucketIsSortedByTime, true);
        ASSERT_EQ(writeBatches[i]->opId, _opCtx->getOpID());
    }
}

TEST_F(TimeseriesWriteOpsInternalTest, PrepareInsertsToBucketsMultipleBucketsOneMeta) {
    auto tsOptions = _getTimeseriesOptions(_ns1);
    std::vector<BSONObj> userBatch;
    for (auto i = 0; i < 2 * gTimeseriesBucketMaxCount; i++) {
        userBatch.emplace_back(BSON(_timeField << Date_t::now() << _metaField << _metaValue));
    }
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;

    AutoGetCollection autoColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();

    auto swWriteBatches = prepareInsertsToBuckets(_opCtx,
                                                  *_bucketCatalog,
                                                  bucketsColl.get(),
                                                  tsOptions,
                                                  _opCtx->getOpID(),
                                                  _getCollator(_ns1),
                                                  _getStorageCacheSizeBytes(),
                                                  /*earlyReturnOnError=*/true,
                                                  _compressBucket,
                                                  userBatch,
                                                  errorsAndIndices);

    ASSERT_TRUE(swWriteBatches.isOK());
    ASSERT_TRUE(errorsAndIndices.empty());

    auto& writeBatches = swWriteBatches.getValue();
    ASSERT_EQ(writeBatches.size(), 2);

    for (size_t i = 0; i < writeBatches.size(); i++) {
        ASSERT_EQ(writeBatches[i]->isReopened, false);
        ASSERT_EQ(writeBatches[i]->bucketIsSortedByTime, true);
        ASSERT_EQ(writeBatches[i]->opId, _opCtx->getOpID());
    }
}

TEST_F(TimeseriesWriteOpsInternalTest, PrepareInsertsToBucketsMultipleBucketsMultipleMetas) {
    auto tsOptions = _getTimeseriesOptions(_ns1);
    std::vector<BSONObj> userBatch;
    for (auto i = 0; i < gTimeseriesBucketMaxCount; i++) {
        userBatch.emplace_back(BSON(_timeField << Date_t::now() << _metaField << _metaValue));
    }
    for (auto i = 0; i < gTimeseriesBucketMaxCount; i++) {
        userBatch.emplace_back(BSON(_timeField << Date_t::now() << _metaField << "m"));
    }
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;

    AutoGetCollection autoColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();

    auto swWriteBatches = prepareInsertsToBuckets(_opCtx,
                                                  *_bucketCatalog,
                                                  bucketsColl.get(),
                                                  tsOptions,
                                                  _opCtx->getOpID(),
                                                  _getCollator(_ns1),
                                                  _getStorageCacheSizeBytes(),
                                                  /*earlyReturnOnError=*/true,
                                                  _compressBucket,
                                                  userBatch,
                                                  errorsAndIndices);

    ASSERT_TRUE(swWriteBatches.isOK());
    ASSERT_TRUE(errorsAndIndices.empty());

    auto& writeBatches = swWriteBatches.getValue();
    ASSERT_EQ(writeBatches.size(), 2);

    for (size_t i = 0; i < writeBatches.size(); i++) {
        ASSERT_EQ(writeBatches[i]->isReopened, false);
        ASSERT_EQ(writeBatches[i]->bucketIsSortedByTime, true);
        ASSERT_EQ(writeBatches[i]->opId, _opCtx->getOpID());
    }
}

TEST_F(TimeseriesWriteOpsInternalTest,
       PrepareInsertsToBucketsMultipleBucketsMultipleMetasInterleaved) {
    auto tsOptions = _getTimeseriesOptions(_ns1);
    std::vector<BSONObj> userBatch;
    for (auto i = 0; i < 2 * gTimeseriesBucketMaxCount; i++) {
        userBatch.emplace_back(BSON(_timeField << Date_t::now() << _metaField
                                               << (i % 2 == 0 ? _metaValue : _metaValue2)));
    }
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;

    AutoGetCollection autoColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();

    auto swWriteBatches = prepareInsertsToBuckets(_opCtx,
                                                  *_bucketCatalog,
                                                  bucketsColl.get(),
                                                  tsOptions,
                                                  _opCtx->getOpID(),
                                                  _getCollator(_ns1),
                                                  _getStorageCacheSizeBytes(),
                                                  /*earlyReturnOnError=*/true,
                                                  _compressBucket,
                                                  userBatch,
                                                  errorsAndIndices);

    ASSERT_TRUE(swWriteBatches.isOK());
    ASSERT_TRUE(errorsAndIndices.empty());

    auto& writeBatches = swWriteBatches.getValue();
    ASSERT_EQ(writeBatches.size(), 2);

    for (size_t i = 0; i < writeBatches.size(); i++) {
        ASSERT_EQ(writeBatches[i]->isReopened, false);
        ASSERT_EQ(writeBatches[i]->bucketIsSortedByTime, true);
        ASSERT_EQ(writeBatches[i]->opId, _opCtx->getOpID());
    }
}

TEST_F(TimeseriesWriteOpsInternalTest, PrepareInsertsBadMeasurementsAll) {
    auto tsOptions = _getTimeseriesOptions(_ns1);
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;
    AutoGetCollection autoColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();

    std::vector<BSONObj> userMeasurementsBatch{
        mongo::fromjson(R"({"tag":"a", "x":2})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"tag":"a", "x":3})"),  // Malformed measurement, missing time field
    };

    auto swWriteBatches = prepareInsertsToBuckets(_opCtx,
                                                  *_bucketCatalog,
                                                  bucketsColl.get(),
                                                  tsOptions,
                                                  _opCtx->getOpID(),
                                                  _getCollator(_ns1),
                                                  _getStorageCacheSizeBytes(),
                                                  /*earlyReturnOnError=*/true,
                                                  _compressBucket,
                                                  userMeasurementsBatch,
                                                  errorsAndIndices);

    ASSERT_FALSE(swWriteBatches.isOK());
    ASSERT_EQ(errorsAndIndices.size(), 2);
}

TEST_F(TimeseriesWriteOpsInternalTest, PrepareInsertsBadMeasurementsSome) {
    auto tsOptions = _getTimeseriesOptions(_ns1);
    std::vector<WriteStageErrorAndIndex> errorsAndIndices;
    AutoGetCollection autoColl(_opCtx, _ns1.makeTimeseriesBucketsNamespace(), MODE_IS);
    const auto& bucketsColl = autoColl.getCollection();

    std::vector<BSONObj> userMeasurementsBatch{
        mongo::fromjson(R"({"tag":"a", "x":2})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:05:00.000Z"}, "tag":"a", "x":3})"),
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:02:00.000Z"}, "tag":"a", "x":3})"),
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T10:03:00.000Z"}, "tag":"b", "x":3})"),
        mongo::fromjson(R"({"tag":"b", "x":3})"),  // Malformed measurement, missing time field
        mongo::fromjson(R"({"time":{"$date":"2025-01-30T09:02:00.000Z"}, "tag":"b", "x":3})"),
    };

    auto swWriteBatches = prepareInsertsToBuckets(_opCtx,
                                                  *_bucketCatalog,
                                                  bucketsColl.get(),
                                                  tsOptions,
                                                  _opCtx->getOpID(),
                                                  _getCollator(_ns1),
                                                  _getStorageCacheSizeBytes(),
                                                  /*earlyReturnOnError=*/true,
                                                  _compressBucket,
                                                  userMeasurementsBatch,
                                                  errorsAndIndices);

    ASSERT_FALSE(swWriteBatches.isOK());
    ASSERT_EQ(errorsAndIndices.size(), 2);

    ASSERT_EQ(errorsAndIndices[0].index, 0);
    ASSERT_EQ(errorsAndIndices[1].index, 4);
}

}  // namespace
}  // namespace mongo::timeseries::write_ops::internal
