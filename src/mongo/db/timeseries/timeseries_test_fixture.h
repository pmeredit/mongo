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

#include <functional>
#include <utility>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/catalog/catalog_test_fixture.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/collation/collator_interface.h"
#include "mongo/db/service_context.h"
#include "mongo/db/timeseries/bucket_catalog/bucket_catalog.h"
#include "mongo/db/timeseries/timeseries_options.h"
#include "mongo/platform/decimal128.h"
#include "mongo/util/uuid.h"

namespace mongo::timeseries {
class TimeseriesTestFixture : public CatalogTestFixture {
public:
    static constexpr uint64_t kDefaultStorageCacheSizeBytes = 1024 * 1024 * 1024;
    static constexpr uint64_t kLimitedStorageCacheSizeBytes = 1024;

protected:
    void setUp() override;
    void tearDown() override;

    virtual std::vector<NamespaceString> getNamespaceStrings();

    virtual BSONObj _makeTimeseriesOptionsForCreate() const;

    virtual BSONObj _makeTimeseriesOptionsForCreateNoMetaField() const;

    virtual void setUpCollectionsHelper(
        std::initializer_list<std::pair<NamespaceString*, UUID*>> collectionMetadata,
        std::function<BSONObj()> makeTimeseriesOptionsForCreateFn);

    void _assertCollWithMetaField(const NamespaceString& ns,
                                  std::vector<BSONObj> batchOfMeasurements) const;

    void _assertCollWithoutMetaField(const NamespaceString& ns,
                                     std::vector<BSONObj> batchOfMeasurements) const;

    void _assertNoMetaFieldsInCollWithMetaField(const NamespaceString& ns,
                                                std::vector<BSONObj> batchOfMeasurements) const;

    TimeseriesOptions _getTimeseriesOptions(const NamespaceString& ns) const;

    const CollatorInterface* _getCollator(const NamespaceString& ns) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue = Date_t::now()) const;

    // We don't supply defaults for these type-specific function declarations because we provide the
    // defaults in the generic _generateMeasurementWithMetaFieldType above.
    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  Timestamp metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  int metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  long long metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  Decimal128 metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  double metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  OID metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  bool metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  BSONBinData metaValue) const;

    BSONObj _generateMeasurementWithMetaFieldType(BSONType type,
                                                  Date_t timeValue,
                                                  StringData metaValue) const;

    struct MeasurementsWithRolloverReasonOptions {
        const bucket_catalog::RolloverReason reason;
        size_t numMeasurements = static_cast<size_t>(gTimeseriesBucketMaxCount);
        size_t idxWithDiffMeasurement = static_cast<size_t>(numMeasurements - 1);
        boost::optional<StringData> metaValue = _metaValue;
        BSONType metaValueType = String;
        Date_t timeValue = Date_t::now();
    };

    std::vector<BSONObj> _generateMeasurementsWithRolloverReason(
        const MeasurementsWithRolloverReasonOptions& options) const;

    uint64_t _getStorageCacheSizeBytes() const;

    template <typename T>
    inline std::vector<T> _getFlattenedVector(const std::vector<std::vector<T>>& vectors) {
        size_t totalSize = std::accumulate(
            vectors.begin(), vectors.end(), size_t(0), [](size_t sum, const std::vector<T>& vec) {
                return sum + vec.size();
            });
        std::vector<T> result;
        result.reserve(totalSize);  // Reserve the total size to avoid multiple allocations
        // Use a range-based for loop to insert elements
        for (const auto& vec : vectors) {
            result.insert(result.end(), vec.begin(), vec.end());
        }
        return result;
    }

    OperationContext* _opCtx;
    bucket_catalog::BucketCatalog* _bucketCatalog;

    static constexpr StringData _timeField = "time";
    static constexpr StringData _metaField = "tag";
    static constexpr StringData _metaValue = "a";
    static constexpr StringData _metaValue2 = "b";
    static constexpr StringData _metaValue3 = "c";
    uint64_t _storageCacheSizeBytes = kDefaultStorageCacheSizeBytes;

    const std::vector<BSONType> _nonStringComponentVariableBSONTypes = {
        bsonTimestamp, NumberInt, NumberLong, NumberDecimal, NumberDouble, jstOID, Bool, BinData};

    const std::vector<BSONType> _stringComponentBSONTypes = {
        Object, Array, RegEx, DBRef, Code, Symbol, CodeWScope, String};

    // These BSONTypes will always return the same meta value when passed in as the BSONType in
    // _generateMeasurementWithMetaFieldType.
    const std::vector<BSONType> _constantBSONTypes = {
        Undefined, Date, MinKey, MaxKey, jstNULL, EOO};

    // Strings used to simulate kSize/kCachePressure rollover reason.
    std::string _bigStr = std::string(1000, 'a');

    NamespaceString _ns1 =
        NamespaceString::createNamespaceString_forTest("timeseries_test_fixture_1", "t_1");
    NamespaceString _ns2 =
        NamespaceString::createNamespaceString_forTest("timeseries_test_fixture_1", "t_2");
    NamespaceString _nsNoMeta = NamespaceString::createNamespaceString_forTest(
        "timeseries_test_fixture_no_meta_field_1", "t_1");

    UUID _uuid1 = UUID::gen();
    UUID _uuid2 = UUID::gen();
    UUID _uuidNoMeta = UUID::gen();

    BSONObj _measurement = BSON(_timeField << Date_t::now() << _metaField << _metaValue);
};
}  // namespace mongo::timeseries
