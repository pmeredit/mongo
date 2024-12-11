/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/group_processor.h"

#include <cstddef>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/group_processor_base.h"

namespace streams {

using namespace mongo;

GroupProcessor::GroupProcessor(mongo::GroupProcessor* processor)
    : mongo::GroupProcessorBase(*processor) {}

boost::optional<GroupProcessor::GroupsMap::iterator> GroupProcessor::findGroup(
    const mongo::Value& key) {
    auto groupIter = _groups.find(key);
    if (groupIter == _groups.end()) {
        return boost::none;
    }
    return boost::make_optional(std::move(groupIter));
}

void GroupProcessor::computeAccumulatorArgs(boost::optional<GroupsMap::iterator> groupIter,
                                            const Document& root,
                                            std::vector<boost::optional<Value>>* accumulatorArgs) {
    const size_t numAccumulators = _accumulatedFields.size();
    accumulatorArgs->clear();
    accumulatorArgs->resize(numAccumulators, boost::none);

    for (size_t i = 0; i < numAccumulators; i++) {
        // Only process the input and update the memory footprint if the current accumulator
        // needs more input.
        if (!groupIter || (*groupIter)->second[i]->needsInput()) {
            accumulatorArgs->at(i) = computeAccumulatorArg(root, i);
        }
    }
}

void GroupProcessor::accumulate(GroupsMap::iterator groupIter,
                                const std::vector<boost::optional<mongo::Value>>& accumulatorArgs) {
    const size_t numAccumulators = _accumulatedFields.size();
    invariant(numAccumulators == accumulatorArgs.size());

    for (size_t i = 0; i < numAccumulators; ++i) {
        if (accumulatorArgs[i]) {
            GroupProcessorBase::accumulate(groupIter, i, *accumulatorArgs[i]);
        }
    }
}

int64_t GroupProcessor::getMemoryUsageBytes() const {
    return getMemoryTracker().currentMemoryBytes();
}

void GroupProcessor::readyGroups() {
    _groupsIterator = _groups.begin();
}

bool GroupProcessor::hasNext() const {
    return _groupsIterator != _groups.end();
}

boost::optional<Document> GroupProcessor::getNext() {
    if (_groupsIterator == _groups.end()) {
        return boost::none;
    }

    Document out = makeDocument(_groupsIterator->first, _groupsIterator->second);
    ++_groupsIterator;
    return out;
}

void GroupProcessor::reset() {
    GroupProcessorBase::reset();
    _groupsIterator = _groups.end();
}

std::pair<mongo::Value, mongo::Value> GroupProcessor::getNextGroup() {
    tassert(8249932, "Iterator should not point to the end", _groupsIterator != _groups.end());
    auto key = _groupsIterator->first;
    const auto& accumulators = _groupsIterator->second;

    std::vector<mongo::Value> mergeableValues;
    mergeableValues.reserve(accumulators.size());
    for (const auto& accumulator : accumulators) {
        auto mergeableValue = accumulator->getValue(/* toBeMerged */ true);
        mergeableValues.push_back(std::move(mergeableValue));
    }

    _groupsIterator++;
    return {std::move(key), Value{std::move(mergeableValues)}};
}

void GroupProcessor::addGroup(Value key,
                              const std::vector<Value>& accumulators,
                              bool allowDuplicateKeys) {
    auto [it, isCreated] = findOrCreateGroup(std::move(key));
    tassert(8249933, "Found a duplicate group key.", isCreated || allowDuplicateKeys);
    for (size_t i = 0; i < accumulators.size(); i++) {
        GroupProcessorBase::accumulate(it, i, accumulators[i]);
    }
}
}  // namespace streams
