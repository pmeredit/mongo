/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/message.h"

namespace streams {

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

};  // namespace streams
