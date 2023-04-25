#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/exec/document_value/document.h"
#include "mongo/util/shared_buffer.h"

namespace streams {

// Indicates whether the input is active or idle.
enum class WatermarkStatus { kActive, kIdle };

// Encapsulates a document read from Kafka and all the metadata for it.
struct KafkaSourceDocument {
    // Exactly one of following 2 fields is ever populated.
    // Contains the BSON document when the input event is successfully parsed.
    boost::optional<mongo::BSONObj> doc;
    // Contains the raw input message when the input event could not be successfully parsed.
    boost::optional<mongo::ConstSharedBuffer> docBuf;

    // Offset of this document within the partition it was read from.
    int64_t offset{0};

    // Size of raw message read from Kafka in bytes.
    int64_t sizeBytes{0};

    // The log append time of this document.
    boost::optional<int64_t> logAppendTimeMs{0};
};

// Encapsulates a document and all the metadata for it.
struct StreamDocument {
    StreamDocument(mongo::Document d) : doc(std::move(d)) {}

    StreamDocument(mongo::Document d,
                   int64_t minProcessingTimeMs,
                   int64_t minEventTimestampMs,
                   int64_t maxEventTimestampMs)
        : doc(std::move(d)),
          minProcessingTimeMs(minProcessingTimeMs),
          minEventTimestampMs(minEventTimestampMs),
          maxEventTimestampMs(maxEventTimestampMs) {}

    // Copy the timing information from another stream document.
    void copyDocumentMetadata(const StreamDocument& other) {
        minEventTimestampMs = other.minEventTimestampMs;
        maxEventTimestampMs = other.maxEventTimestampMs;
        minProcessingTimeMs = other.minProcessingTimeMs;
    }

    mongo::Document doc;

    // The minimum processing time of input documents consumed to produce
    // the document above.
    int64_t minProcessingTimeMs{0};

    // The minimum event timestamp of input documents consumed to produce
    // the document above.
    int64_t minEventTimestampMs{0};

    // The maximum event timestamp of input documents consumed to produce
    // the document above.
    int64_t maxEventTimestampMs{0};
};

// Encapsulates the data we want to send from an operator to the next operator.
struct StreamDataMsg {
    std::vector<StreamDocument> docs;
};

// Encapsulates the watermark related metadata we want to send from an operator to the next
// operator.
struct WatermarkControlMsg {
    WatermarkStatus watermarkStatus{WatermarkStatus::kActive};

    // Watermark of the sender operator in milliseconds.
    // This should only be used when watermarkStatus is kActive.
    int64_t eventTimeWatermarkMs{0};

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

// Encapsulates any control messages we want to send from an operator to the next operator.
struct StreamControlMsg {
    boost::optional<WatermarkControlMsg> watermarkMsg;

    bool operator==(const StreamControlMsg& other) const {
        if (watermarkMsg && other.watermarkMsg) {
            return *watermarkMsg == *other.watermarkMsg;
        }
        return bool(watermarkMsg) == bool(other.watermarkMsg);
    }

    bool operator!=(const StreamControlMsg& other) const {
        return !operator==(other);
    }
};

// Encapsulates StreamDataMsg and StreamControlMsg.
struct StreamMsgUnion {
    boost::optional<StreamDataMsg> dataMsg;
    boost::optional<StreamControlMsg> controlMsg;
};

}  // namespace streams
