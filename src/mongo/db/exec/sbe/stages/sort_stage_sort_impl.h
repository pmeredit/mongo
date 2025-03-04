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

#include <memory>
#include <vector>

#include "mongo/db/exec/sbe/expressions/compile_ctx.h"
#include "mongo/db/exec/sbe/stages/sort.h"
#include "mongo/db/exec/sbe/values/row.h"
#include "mongo/db/sorter/sorter.h"
#include "mongo/db/sorter/sorter_template_defs.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"

namespace mongo::sbe {

template <typename KeyRow, typename ValueRow>
class SortStage::SortImpl : public SortIface {
public:
    explicit SortImpl(SortStage& stage) : _stage(stage) {}

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

private:
    int64_t runLimitCode();
    void makeSorter();

    using SorterIterator = SortIteratorInterface<KeyRow, ValueRow>;
    using SorterData = std::pair<KeyRow, ValueRow>;

    SortStage& _stage;

    std::vector<value::SlotAccessor*> _inKeyAccessors;
    std::vector<value::SlotAccessor*> _inValueAccessors;

    value::SlotMap<std::unique_ptr<value::SlotAccessor>> _outAccessors;

    std::unique_ptr<SorterIterator> _mergeIt;
    SorterData _mergeData{};
    SorterData* _mergeDataIt{&_mergeData};
    std::unique_ptr<Sorter<KeyRow, ValueRow>> _sorter;

    std::unique_ptr<vm::CodeFragment> _limitCode;
};

template <typename KeyRow, typename ValueRow>
void SortStage::SortImpl<KeyRow, ValueRow>::prepare(CompileCtx& ctx) {
    _stage._children[0]->prepare(ctx);

    size_t counter = 0;
    // Process order by fields.
    for (auto& slot : _stage._obs) {
        _inKeyAccessors.emplace_back(_stage._children[0]->getAccessor(ctx, slot));
        auto [it, inserted] =
            _outAccessors.emplace(slot,
                                  std::make_unique<value::MaterializedRowKeyAccessor<SorterData*>>(
                                      _mergeDataIt, counter));
        ++counter;
        uassert(4822812, str::stream() << "duplicate field: " << slot, inserted);
    }

    counter = 0;
    // Process value fields.
    for (auto& slot : _stage._vals) {
        _inValueAccessors.emplace_back(_stage._children[0]->getAccessor(ctx, slot));
        auto [it, inserted] = _outAccessors.emplace(
            slot,
            std::make_unique<value::MaterializedRowValueAccessor<SorterData*>>(_mergeDataIt,
                                                                               counter));
        ++counter;
        uassert(4822813, str::stream() << "duplicate field: " << slot, inserted);
    }

    if (_stage._limitExpr) {
        _limitCode = _stage._limitExpr->compile(ctx);
    }
}

template <typename KeyRow, typename ValueRow>
value::SlotAccessor* SortStage::SortImpl<KeyRow, ValueRow>::getAccessor(CompileCtx& ctx,
                                                                        value::SlotId slot) {
    if (auto it = _outAccessors.find(slot); it != _outAccessors.end()) {
        return it->second.get();
    }

    return ctx.getAccessor(slot);
}

template <typename KeyRow, typename ValueRow>
int64_t SortStage::SortImpl<KeyRow, ValueRow>::runLimitCode() {
    auto [owned, tag, val] = vm::ByteCode{}.run(_limitCode.get());
    value::ValueGuard guard{owned, tag, val};
    tassert(8349205, "Limit code returned unexpected value", tag == value::TypeTags::NumberInt64);
    return value::bitcastTo<size_t>(val);
}

template <typename KeyRow, typename ValueRow>
void SortStage::SortImpl<KeyRow, ValueRow>::makeSorter() {
    SortOptions opts;
    opts.tempDir = storageGlobalParams.dbpath + "/_tmp";
    opts.maxMemoryUsageBytes = _stage._specificStats.maxMemoryUsageBytes;
    opts.extSortAllowed = _stage._allowDiskUse;
    opts.limit = _stage._specificStats.limit != std::numeric_limits<size_t>::max()
        ? _stage._specificStats.limit
        : 0;
    opts.moveSortedDataIntoIterator = true;
    if (_stage._allowDiskUse) {
        _stage._sorterFileStats = std::make_unique<SorterFileStats>(nullptr);
        opts.sorterFileStats = _stage._sorterFileStats.get();
    }

    auto comp = [&](const KeyRow& lhs, const KeyRow& rhs) {
        auto size = lhs.size();
        for (size_t idx = 0; idx < size; ++idx) {
            auto [lhsTag, lhsVal] = lhs.getViewOfValue(idx);
            auto [rhsTag, rhsVal] = rhs.getViewOfValue(idx);
            auto [tag, val] = value::compareValue(lhsTag, lhsVal, rhsTag, rhsVal);
            uassert(7086700, "Invalid comparison result", tag == value::TypeTags::NumberInt32);
            auto result = value::bitcastTo<int32_t>(val);
            if (result) {
                return _stage._dirs[idx] == value::SortDirection::Descending ? -result : result;
            }
        }

        return 0;
    };

    _sorter = Sorter<KeyRow, ValueRow>::make(opts, comp, {});
    _mergeIt.reset();
}

template <typename KeyRow, typename ValueRow>
void SortStage::SortImpl<KeyRow, ValueRow>::open(bool reOpen) {
    auto optTimer(_stage.getOptTimer(_stage._opCtx));

    invariant(_stage._opCtx);
    _stage._commonStats.opens++;
    _stage._children[0]->open(reOpen);

    if (_limitCode) {
        _stage._specificStats.limit = runLimitCode();
    } else {
        _stage._specificStats.limit = std::numeric_limits<size_t>::max();
    }

    makeSorter();

    while (_stage._children[0]->getNext() == PlanState::ADVANCED) {
        KeyRow keys{_inKeyAccessors.size()};

        size_t idx = 0;
        for (auto accessor : _inKeyAccessors) {
            auto [tag, val] = accessor->getViewOfValue();
            keys.reset(idx++, false, tag, val);
        }

        // Do not allocate the values here, instead let the sorter decide, since the sorter may
        // decide not to store the values in the case of sort-limit.
        _sorter->emplace(std::move(keys), [&]() {
            ValueRow vals{_inValueAccessors.size()};
            size_t idx = 0;
            for (auto accessor : _inValueAccessors) {
                auto [tag, val] = accessor->getViewOfValue();
                vals.reset(idx++, false, tag, val);
            }
            return vals;
        });
    }

    _stage._specificStats.totalDataSizeBytes += _sorter->stats().bytesSorted();
    _mergeIt = _sorter->done();
    _stage._specificStats.spillingStats.incrementSpills(_sorter->stats().spilledRanges());
    _stage._specificStats.spillingStats.incrementSpilledRecords(
        _sorter->stats().spilledKeyValuePairs());
    _stage._specificStats.keysSorted += _sorter->stats().numSorted();
    if (_stage._sorterFileStats) {
        _stage._specificStats.spillingStats.incrementSpilledDataStorageSize(
            _stage._sorterFileStats->bytesSpilled());
        _stage._specificStats.spillingStats.incrementSpilledBytes(
            _stage._sorterFileStats->bytesSpilledUncompressed());
    }

    _stage._children[0]->close();
}

template <typename KeyRow, typename ValueRow>
PlanState SortStage::SortImpl<KeyRow, ValueRow>::getNext() {
    auto optTimer(_stage.getOptTimer(_stage._opCtx));
    _stage.checkForInterruptAndYield(_stage._opCtx);

    // When the sort spilled data to disk then read back the sorted runs.
    if (_mergeIt && _mergeIt->more()) {
        _mergeData = _mergeIt->next();

        return _stage.trackPlanState(PlanState::ADVANCED);
    } else {
        return _stage.trackPlanState(PlanState::IS_EOF);
    }
}

template <typename KeyRow, typename ValueRow>
void SortStage::SortImpl<KeyRow, ValueRow>::close() {
    auto optTimer(_stage.getOptTimer(_stage._opCtx));

    _stage.trackClose();
    _mergeIt.reset();
    _sorter.reset();
}

// Extern specializations to conserve compiler memory.
// Cross-product of `[FixedSizeRow<1>, FixedSizeRow<2>, FixedSizeRow<3>, MaterializedRow]`
// by `[FixedSizeRow<1>, MaterializedRow]`.
extern template class SortStage::SortImpl<value::FixedSizeRow<1>, value::FixedSizeRow<1>>;
extern template class SortStage::SortImpl<value::FixedSizeRow<1>, value::MaterializedRow>;
extern template class SortStage::SortImpl<value::FixedSizeRow<2>, value::FixedSizeRow<1>>;
extern template class SortStage::SortImpl<value::FixedSizeRow<2>, value::MaterializedRow>;
extern template class SortStage::SortImpl<value::FixedSizeRow<3>, value::FixedSizeRow<1>>;
extern template class SortStage::SortImpl<value::FixedSizeRow<3>, value::MaterializedRow>;
extern template class SortStage::SortImpl<value::MaterializedRow, value::FixedSizeRow<1>>;
extern template class SortStage::SortImpl<value::MaterializedRow, value::MaterializedRow>;

}  // namespace mongo::sbe
