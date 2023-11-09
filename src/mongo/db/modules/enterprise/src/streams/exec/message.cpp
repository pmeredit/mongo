/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/message.h"
#include "mongo/db/exec/document_value/document_comparator.h"

namespace streams {

using namespace mongo;

bool StreamControlMsg::operator==(const StreamControlMsg& other) const {
    auto hasSameFieldsSet = bool(watermarkMsg) == bool(other.watermarkMsg) &&
        bool(checkpointMsg) == bool(other.checkpointMsg) && eofSignal == other.eofSignal;
    if (!hasSameFieldsSet) {
        return false;
    }
    if (watermarkMsg && (*watermarkMsg != *other.watermarkMsg)) {
        return false;
    }
    if (checkpointMsg && (*checkpointMsg != *other.checkpointMsg)) {
        return false;
    }
    // pushDocumentSourceEofSingal equality was already checked above.
    return true;
}

bool StreamDocument::operator==(const StreamDocument& other) const {
    mongo::DocumentComparator compare;
    return compare.evaluate(doc == other.doc) &&
        compare.evaluate(mongo::Document{streamMeta.toBSON()} ==
                         mongo::Document{other.streamMeta.toBSON()}) &&
        minProcessingTimeMs == other.minProcessingTimeMs &&
        minEventTimestampMs == other.minEventTimestampMs &&
        maxEventTimestampMs == other.maxEventTimestampMs;
}

};  // namespace streams
