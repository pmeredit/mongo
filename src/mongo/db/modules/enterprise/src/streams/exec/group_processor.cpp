/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/group_processor.h"

#include <limits>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/exec/document_value/value_comparator.h"
#include "mongo/db/pipeline/accumulation_statement.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
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

boost::optional<Document> GroupProcessor::getNext() {
    if (_groupsIterator == _groups.end())
        return boost::none;

    Document out =
        makeDocument(_groupsIterator->first, _groupsIterator->second, _expCtx->needsMerge);
    ++_groupsIterator;
    return out;
}

void GroupProcessor::reset() {
    GroupProcessorBase::reset();
    _groupsIterator = _groups.end();
}

}  // namespace streams
