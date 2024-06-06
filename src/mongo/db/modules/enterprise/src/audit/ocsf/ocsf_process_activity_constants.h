/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#pragma once

#include "mongo/base/string_data.h"
#include "mongo/platform/basic.h"

namespace mongo::audit {

constexpr auto kStatusId = "status_id"_sd;
constexpr auto kDataId = "data"_sd;
constexpr auto kEnrichmentsId = "enrichments"_sd;
constexpr auto kObservablesId = "observables"_sd;
constexpr auto kNameId = "name"_sd;
constexpr StringData kActivityName = "Launch"_sd;

// activityId
constexpr int kProcessActivityLaunch = 1;
constexpr int kProcessActivityTerminate = 2;
constexpr int kProcessActivityOther = 99;

// observables
constexpr auto kTypeId = "type_id"_sd;
constexpr auto kValueId = "value"_sd;
constexpr auto kTypeFileName = 7;
constexpr auto kTypeOther = 99;

}  // namespace mongo::audit
