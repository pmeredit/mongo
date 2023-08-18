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

    OperatorStats& operator+=(const OperatorStats& other) {
        numInputDocs += other.numInputDocs;
        numInputBytes += other.numInputBytes;
        numOutputDocs += other.numOutputDocs;
        numOutputBytes += other.numOutputBytes;
        numDlqDocs += other.numDlqDocs;
        return *this;
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
};

}  // namespace streams
