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

}  // namespace mongotmock
}  // namespace mongo
