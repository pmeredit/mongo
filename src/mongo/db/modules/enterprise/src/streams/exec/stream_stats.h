#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

namespace streams {

// Encapsulates stats for an Operator.
struct OperatorStats {
    std::string operatorName;
    // Input/output byte stats are only computed for source/sink operators as it is expensive to
    // compute byte sizes of documents.
    int64_t numInputDocs{0};
    int64_t numInputBytes{0};
    int64_t numOutputDocs{0};
    int64_t numOutputBytes{0};
    // Rejected documents that were added to the dead letter queue.
    // This is only computed for source operators.
    int64_t numDlqDocs{0};
    // Amount of memory (in bytes) that the operator is actively using.
    // This only applies to stateful operators (e.g. group operator).
    int64_t memoryUsageBytes{0};

    OperatorStats& operator+=(const OperatorStats& other) {
        numInputDocs += other.numInputDocs;
        numInputBytes += other.numInputBytes;
        numOutputDocs += other.numOutputDocs;
        numOutputBytes += other.numOutputBytes;
        numDlqDocs += other.numDlqDocs;
        memoryUsageBytes += other.memoryUsageBytes;
        return *this;
    }

    // Returns only stats that are additive and sets non-additive stats to zero.
    // Currently just clears memoryUsageBytes, the other stats like inputDocs are additive.
    OperatorStats getAdditiveStats() const {
        auto stats = *this;
        stats.memoryUsageBytes = 0;
        return stats;
    }
};

// Encapsulates detailed stats for a stream.
struct StreamStats {
    // The OperatorStats in this vector should be in the order the Operators appear
    // in the OperatorDag. Thus, the first entry should be for the SourceOperator and
    // the last entry should be for the SinkOperator.
    std::vector<OperatorStats> operatorStats;
};

// Encapsulates summarized stats for a stream.
struct StreamSummaryStats {
    int64_t numInputDocs{0};
    int64_t numOutputDocs{0};
    int64_t numInputBytes{0};
    int64_t numOutputBytes{0};
    int64_t memoryUsageBytes{0};
};

// Returns the summary stats based on the per-operator stats passed in. The stream summary
// stats are the input and output of the streaming pipeline as a whole.
StreamSummaryStats computeStreamSummaryStats(const std::vector<OperatorStats>& operatorStats);

}  // namespace streams
