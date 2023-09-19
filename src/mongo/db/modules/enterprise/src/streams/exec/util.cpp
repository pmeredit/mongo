/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/util.h"
#include "streams/exec/constants.h"

using namespace mongo;

namespace streams {

bool isSourceStage(mongo::StringData name) {
    return name == mongo::StringData(kSourceStageName);
}

bool isSinkStage(mongo::StringData name) {
    return isEmitStage(name) || isMergeStage(name);
}

bool isWindowStage(mongo::StringData name) {
    return name == kTumblingWindowStageName || name == kHoppingWindowStageName;
}

bool isLookUpStage(mongo::StringData name) {
    return name == mongo::StringData(kLookUpStageName);
}

bool isEmitStage(mongo::StringData name) {
    return name == mongo::StringData(kEmitStageName);
}

bool isMergeStage(mongo::StringData name) {
    return name == mongo::StringData(kMergeStageName);
}

// TODO(STREAMS-220)-PrivatePreview: Especially with units of day and year,
// this logic probably leads to incorrect for daylights savings time and leap years.
// In future work we can rely more on std::chrono for the time math here, for now
// we're just converting the size and slide to milliseconds for simplicity.
int64_t toMillis(mongo::StreamTimeUnitEnum unit, int count) {
    switch (unit) {
        case StreamTimeUnitEnum::Millisecond:
            return stdx::chrono::milliseconds(count).count();
        case StreamTimeUnitEnum::Second:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::seconds(count))
                .count();
        case StreamTimeUnitEnum::Minute:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::minutes(count))
                .count();
        case StreamTimeUnitEnum::Hour:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::hours(count))
                .count();
        // For Day, using the C++20 ratios from
        // https://en.cppreference.com/w/cpp/header/chrono
        case StreamTimeUnitEnum::Day:
            return stdx::chrono::duration_cast<stdx::chrono::milliseconds>(
                       stdx::chrono::seconds(86400 * count))
                .count();
        default:
            MONGO_UNREACHABLE;
    }
}

BSONObjBuilder toDeadLetterQueueMsg(StreamMeta streamMeta, boost::optional<std::string> error) {
    BSONObjBuilder objBuilder;
    objBuilder.append(kStreamsMetaField, streamMeta.toBSON());
    if (error) {
        objBuilder.append("errInfo", BSON("reason" << *error));
    }
    return objBuilder;
}

BSONObjBuilder toDeadLetterQueueMsg(StreamDocument streamDoc, boost::optional<std::string> error) {
    BSONObjBuilder objBuilder =
        toDeadLetterQueueMsg(std::move(streamDoc.streamMeta), std::move(error));
    objBuilder.append("fullDocument", streamDoc.doc.toBson());
    return objBuilder;
}

}  // namespace streams
