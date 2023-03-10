/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongotmock_state.h"

namespace mongo {

namespace mongotmock {

namespace {
const auto getMongotMockStateDecoration = ServiceContext::declareDecoration<MongotMockState>();
}

MongotMockStateGuard getMongotMockState(ServiceContext* svc) {
    auto& mockState = getMongotMockStateDecoration(svc);
    return MongotMockStateGuard(&mockState);
}

bool checkUserCommandMatchesExpectedCommand(const BSONObj& userCmd, const BSONObj& expectedCmd) {
    // Check that the given command matches the expected command's values.
    for (auto&& elem : expectedCmd) {
        if (!SimpleBSONElementComparator::kInstance.evaluate(userCmd[elem.fieldNameStringData()] ==
                                                             elem)) {
            return false;
        }
    }
    return true;
}

}  // namespace mongotmock
}  // namespace mongo
