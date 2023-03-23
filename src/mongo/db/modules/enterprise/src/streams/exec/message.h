#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/exec/document_value/document.h"

namespace streams {

// Indicates whether the input is active or idle.
enum class WatermarkStatus { kActive, kIdle };

// Encapsulates a document read from Kafka and all the metadata for it.
struct KafkaSourceDocument {
    mongo::BSONObj doc;

    // Offset of this document within the partition it was read from.
    int64_t offset;

    // The log append time of this document.
    int64_t logAppendTimeMs{0};
};

// Encapsulates a document and all the metadata for it.
struct StreamDocument {
    StreamDocument(mongo::Document d) : doc(std::move(d)) {}

    mongo::Document doc;

    // The minimum ingestion time of input documents consumed to produce
    // the document above.
    int64_t minIngestionTimeMs{0};

    // The minimum event time of input documents consumed to produce
    // the document above.
    int64_t minEventTimeMs{0};

    // The maximum event time of input documents consumed to produce
    // the document above.
    int64_t maxEventTimeMs{0};
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
};

// Encapsulates any control messages we want to send from an operator to the next operator.
struct StreamControlMsg {
    boost::optional<WatermarkControlMsg> watermarkMsg;
};

// Encapsulates StreamDataMsg and StreamControlMsg.
struct StreamMsgUnion {
    boost::optional<StreamDataMsg> dataMsg;
    boost::optional<StreamControlMsg> controlMsg;
};

}  // namespace streams
