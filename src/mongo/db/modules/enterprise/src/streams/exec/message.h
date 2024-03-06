#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/exec/document_value/document.h"
#include "streams/exec/exec_internal_gen.h"

namespace streams {

/**
 * Used to identify operators in checkpoint data.
 * Each Operator in the DAG receives a unique OperatorId.
 * This includes Operators in a window's inner pipeline.
 */
using OperatorId = int32_t;

/**
 * Timestamp used to identify a checkpoint.
 */
using CheckpointId = int64_t;

// Indicates whether the input is active or idle.
enum class WatermarkStatus { kActive, kIdle };

// Encapsulates a document read from Kafka and all the metadata for it.
struct KafkaSourceDocument {
    // Exactly one of following 2 fields is ever populated.
    // Contains the BSON document when the input event is successfully parsed.
    boost::optional<mongo::BSONObj> doc;
    // Contains the error message when the input event could not be successfully parsed.
    boost::optional<std::string> error;

    // The partition this document was read from.
    int32_t partition{0};

    // Offset of this document within the partition.
    int64_t offset{0};

    // Size of raw message read from Kafka in bytes.
    int64_t sizeBytes{0};

    // The log append time of this document.
    boost::optional<int64_t> logAppendTimeMs{0};
};

// Encapsulates a document and all the metadata for it.
struct StreamDocument {
    StreamDocument(mongo::Document d) : doc(std::move(d)) {}

    StreamDocument(mongo::Document d, int64_t minEventTimestampMs)
        : doc(std::move(d)), minEventTimestampMs(minEventTimestampMs) {}

    // Copy the timing information from another stream document.
    void copyDocumentMetadata(const StreamDocument& other) {
        streamMeta = other.streamMeta;
        minProcessingTimeMs = other.minProcessingTimeMs;
        minEventTimestampMs = other.minEventTimestampMs;
        maxEventTimestampMs = other.maxEventTimestampMs;
    }

    mongo::Document doc;

    mongo::StreamMeta streamMeta;

    // The minimum processing time of input documents consumed to produce
    // the document above.
    int64_t minProcessingTimeMs{-1};

    // The minimum event timestamp of input documents consumed to produce
    // the document above.
    int64_t minEventTimestampMs{-1};

    // The maximum event timestamp of input documents consumed to produce
    // the document above.
    int64_t maxEventTimestampMs{-1};

    // Only used for testing purposes.
    bool operator==(const StreamDocument& other) const;

    bool operator!=(const StreamDocument& other) const {
        return !operator==(other);
    }
};

// Encapsulates the data we want to send from an operator to the next operator.
struct StreamDataMsg {
    std::vector<StreamDocument> docs;

    int64_t getSizeBytes() const {
        int64_t out{0};
        for (const auto& doc : docs) {
            out += doc.doc.getCurrentApproximateSize();
        }
        return out;
    }

    // Only used for testing purposes.
    bool operator==(const StreamDataMsg& other) const {
        if (docs.size() != other.docs.size()) {
            return false;
        }
        for (size_t i = 0; i < docs.size(); ++i) {
            if (docs[i] != other.docs[i]) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(const StreamDataMsg& other) const {
        return !operator==(other);
    }

    // Only used for serialization in LOGV2 statements.
    mongo::BSONObj toBSONForLogging() const;
};

// Encapsulates the watermark related metadata we want to send from an operator to the next
// operator.
struct WatermarkControlMsg {
    WatermarkStatus watermarkStatus{WatermarkStatus::kActive};

    // Watermark of the sender operator in milliseconds.
    // This should only be used when watermarkStatus is kActive.
    int64_t eventTimeWatermarkMs{-1};

    bool operator==(const WatermarkControlMsg& other) const {
        if (watermarkStatus != other.watermarkStatus) {
            return false;
        }
        if (eventTimeWatermarkMs != other.eventTimeWatermarkMs) {
            return false;
        }
        return true;
    }

    bool operator!=(const WatermarkControlMsg& other) const {
        return !operator==(other);
    }
};

// The control message sent through the DAG during a checkpoint.
struct CheckpointControlMsg {
    CheckpointId id;

    bool operator==(const CheckpointControlMsg& other) const {
        return id == other.id;
    }

    bool operator!=(const CheckpointControlMsg& other) const {
        return !operator==(other);
    }
};

// Encapsulates any control messages we want to send from an operator to the next operator.
struct StreamControlMsg {
    boost::optional<WatermarkControlMsg> watermarkMsg;

    boost::optional<CheckpointControlMsg> checkpointMsg;

    // Indicates EOF to the blocking (i.e. StreamType::kBlocking) stages / operators.
    // This is currently only used in the inner pipeline of a window stage.
    bool eofSignal{false};

    // If set, this specifies the start time (in millis) of the window that should be closed.
    boost::optional<int64_t> windowCloseSignal;

    bool empty() const {
        return *this == StreamControlMsg{};
    }

    bool operator==(const StreamControlMsg& other) const;

    bool operator!=(const StreamControlMsg& other) const {
        return !operator==(other);
    }

    // Only used for serialization in LOGV2 statements.
    mongo::BSONObj toBSONForLogging() const;
};

// Encapsulates StreamDataMsg and StreamControlMsg.
struct StreamMsgUnion {
    boost::optional<StreamDataMsg> dataMsg;
    boost::optional<StreamControlMsg> controlMsg;

    // Only used for testing purposes.
    bool operator==(const StreamMsgUnion& other) const {
        if (bool(dataMsg) != bool(other.dataMsg)) {
            return false;
        }
        if (bool(controlMsg) != bool(other.controlMsg)) {
            return false;
        }
        if (dataMsg && *dataMsg != *other.dataMsg) {
            return false;
        }
        if (controlMsg && *controlMsg != *other.controlMsg) {
            return false;
        }
        return true;
    }

    bool operator!=(const StreamMsgUnion& other) const {
        return !operator==(other);
    }
};

}  // namespace streams
