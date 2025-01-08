/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/message.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
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
        minDocTimestampMs == other.minDocTimestampMs &&
        maxDocTimestampMs == other.maxDocTimestampMs;
}

BSONObj StreamDataMsg::toBSONForLogging() const {
    BSONObjBuilder builder;
    builder.append("byteSize", getByteSize());
    builder.append("docCount", (int64_t)docs.size());
    auto arrayBuilder =
        std::make_unique<BSONArrayBuilder>(BSONArrayBuilder(builder.subarrayStart("data")));
    const int64_t maxLogSize = 1'000'000;
    int64_t size{0};
    for (auto& doc : docs) {
        BSONObjBuilder docBuilder;
        docBuilder.append("doc", doc.doc.toBson());
        docBuilder.append("meta", doc.streamMeta.toBSON());
        auto obj = docBuilder.obj();
        size += obj.objsize();
        if (size > maxLogSize) {
            builder.append("truncated", true);
            break;
        } else {
            arrayBuilder->append(std::move(obj));
        }
    }
    arrayBuilder->doneFast();
    return builder.obj();
}

BSONObj StreamControlMsg::toBSONForLogging() const {
    BSONObjBuilder builder;
    if (eofSignal) {
        builder.append("eofSignal", eofSignal);
    }
    if (checkpointMsg) {
        builder.append("checkpointMsg.checkpointId", checkpointMsg->id);
    }
    if (watermarkMsg) {
        builder.append("watermarkMsg.watermarkEventTime", watermarkMsg->watermarkTimestampMs);
        builder.append("watermarkMsg.watermarkStatus", watermarkMsg->watermarkStatus);
    }
    if (windowCloseSignal) {
        builder.append("windowCloseSignal.Partition", windowCloseSignal->partition.toString());
        builder.append("windowCloseSignal.WindowStartTime", windowCloseSignal->windowStartTime);
        builder.append("windowCloseSignal.WindowEndTime", windowCloseSignal->windowEndTime);
        builder.append("windowCloseSignal.WindowId", windowCloseSignal->windowId);
    }
    return builder.obj();
}

};  // namespace streams
