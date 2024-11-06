/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/modules/enterprise/src/streams/exec/stages_gen.h"
#include "mongo/util/duration.h"
#include "mongo/util/timer.h"

namespace streams {
/**
 * Calculates TimeSpent based on a decay factor.
 *   uses exponential decay based on elapsed time.  The decay is such that the readings older than 5
 * minutes would contribute to less than 1 percent.
 */
mongo::Microseconds calculateDecayTimeSpent(int64_t currentTimeSpentMicros,
                                            int64_t previousTimeSpentMicros,
                                            int64_t timeDeltaMillis);

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
    int64_t numDlqDocs{0};
    int64_t numDlqBytes{0};
    // Amount of memory (in bytes) that the operator is actively using.
    // This only applies to stateful operators (e.g. group operator).
    int64_t memoryUsageBytes{0};
    // Max amount of memory (in bytes) used thus far.
    int64_t maxMemoryUsageBytes{0};
    // Total execution time (in microseconds) for the operator.
    mongo::Microseconds executionTime{0};
    // watermark timestamp for the operator
    // right now supported only for source
    int64_t watermark{-1};

    // time spent in this operator
    mongo::Microseconds timeSpent{0};
    // lastTimeSpentUpdated timer to be able to decay previous value.
    mongo::Timer lastTimeSpentUpdated;

    // The minimum open window's start time. Only set in window assigning operator.
    boost::optional<mongo::Date_t> minOpenWindowStartTime;
    // The maximum open window's start time. Only set in window assigning operator.
    boost::optional<mongo::Date_t> maxOpenWindowStartTime;
    boost::optional<mongo::ConnectionTypeEnum> connectionType;

    void setMemoryUsageBytes(int64_t bytes) {
        memoryUsageBytes = bytes;
        maxMemoryUsageBytes = std::max(maxMemoryUsageBytes, memoryUsageBytes);
    }

    OperatorStats& operator+=(const OperatorStats& other) {
        numInputDocs += other.numInputDocs;
        numInputBytes += other.numInputBytes;
        numOutputDocs += other.numOutputDocs;
        numOutputBytes += other.numOutputBytes;
        numDlqDocs += other.numDlqDocs;
        numDlqBytes += other.numDlqBytes;
        memoryUsageBytes += other.memoryUsageBytes;
        maxMemoryUsageBytes =
            std::max(maxMemoryUsageBytes, std::max(memoryUsageBytes, other.maxMemoryUsageBytes));
        executionTime += other.executionTime;
        // timeSpent needs to be updated whenever an output batch is emitted,
        // using the arrival time of the batch.
        // For windows, we use the time when the window is created.
        if (other.timeSpent > mongo::Microseconds(0)) {
            auto elapsed = lastTimeSpentUpdated.millis();
            timeSpent =
                calculateDecayTimeSpent(other.timeSpent.count(), timeSpent.count(), elapsed);
            lastTimeSpentUpdated.reset();
        }
        // watermark is not updated here intentionally.
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
    // watermark timestamp for the operator
    // right now supported only for source
    int64_t watermark{-1};
    int64_t numDlqDocs{0};
    int64_t numDlqBytes{0};

    StreamSummaryStats& operator-=(const StreamSummaryStats& other) {
        numInputDocs -= other.numInputDocs;
        numInputBytes -= other.numInputBytes;
        numOutputDocs -= other.numOutputDocs;
        numOutputBytes -= other.numOutputBytes;
        memoryUsageBytes -= other.memoryUsageBytes;
        numDlqDocs -= other.numDlqDocs;
        numDlqBytes -= other.numDlqBytes;
        return *this;
    }

    StreamSummaryStats& operator+=(const StreamSummaryStats& other) {
        numInputDocs += other.numInputDocs;
        numInputBytes += other.numInputBytes;
        numOutputDocs += other.numOutputDocs;
        numOutputBytes += other.numOutputBytes;
        numDlqDocs += other.numDlqDocs;
        numDlqBytes += other.numDlqBytes;
        memoryUsageBytes += other.memoryUsageBytes;
        return *this;
    }

    StreamSummaryStats operator-(const StreamSummaryStats& rhs) {
        auto clone = *this;
        clone -= rhs;
        return clone;
    }
};

// Returns the summary stats based on the per-operator stats passed in. The stream summary
// stats are the input and output of the streaming pipeline as a whole.
StreamSummaryStats computeStreamSummaryStats(const std::vector<OperatorStats>& operatorStats);

// Kafka consumer partition state.
struct KafkaConsumerPartitionState {
    // Topic that this consumer is subscribed to
    std::string topic;

    // Partition ID that this state represents.
    int32_t partition{0};

    // The offset that the stream processor is currently on for this partition. This is
    // the last offset that was processed by the stream processor plus one.
    int64_t currentOffset{0};

    // The offset that the stream processor last committed to the kafka broker and checkpoint
    // for this partition.
    int64_t checkpointOffset{0};

    // This is the lag of the current offset from the latest offset in the broker
    boost::optional<int64_t> partitionOffsetLag;

    // watermark timestamp for the partition
    int64_t watermark{-1};

    // True if the partition is idle.
    bool isIdle{false};
};  // struct KafkaConsumerPartitionState

}  // namespace streams
