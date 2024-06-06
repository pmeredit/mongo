/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/pipeline/group_processor.h"
#include "mongo/db/pipeline/group_processor_base.h"

namespace streams {

/**
 * This class provides the underlying grouping and aggregation functionality needed by
 * GroupOperator. We need this class because mongo::GroupProcessor exposes a different
 * and slightly more efficient interface for DocumentSourceGroup.
 */
class GroupProcessor : public mongo::GroupProcessorBase {
public:
    // Uses the given mongo::GroupProcessor instance to intialize the base class.
    GroupProcessor(mongo::GroupProcessor* processor);

    // Finds the group for the given key. Returns boost::none when the group does not already exist.
    // Note that this method does not insert a new group when the group does not already exist.
    boost::optional<GroupsMap::iterator> findGroup(const mongo::Value& key);

    // Computes the accumulator arguments for the given document. Only those arguments are computed
    // that would be needed as per AccumulatorState::needsInput().
    void computeAccumulatorArgs(boost::optional<GroupsMap::iterator> groupIter,
                                const mongo::Document& root,
                                std::vector<boost::optional<mongo::Value>>* accumulatorArgs);

    // Adds the given accumulator arguments to the given group.
    void accumulate(GroupsMap::iterator groupIter,
                    const std::vector<boost::optional<mongo::Value>>& accumulatorArgs);

    // Prepares internal state to start returning fully aggregated groups back to the caller via
    // getNext() calls. Note that accumulate() must not be called after this method is called.
    void readyGroups();

    // Returns whether or not there is a next aggregated result document when `getNext()` is called.
    bool hasNext() const;

    // Returns the next aggregated result document. Returns boost::none if there are no more
    // documents to return. Note that this must be called after readyGroups() has already been
    // called once.
    boost::optional<mongo::Document> getNext();

    // Resets the internal state to match the initial state.
    void reset();

    // Returns the memory that is actively being used for the group processor, in bytes.
    int64_t getMemoryUsageBytes() const;

    // returns a pair of group key and the corresponding accumulators
    std::pair<mongo::Value, mongo::Value> getNextGroup();

    // Add a new group (key and accumulators) state. Only used by the group operator recovery path
    void addGroup(mongo::Value key, const std::vector<mongo::Value>& accumulators);

private:
    GroupProcessorBase::GroupsMap::iterator _groupsIterator;
};

}  // namespace streams
