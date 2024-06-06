/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#pragma once

namespace mongo::audit::ocsf {

// Activities
constexpr auto kAccountChangeActivityUnknown = 0;
constexpr auto kAccountChangeActivityCreate = 1;
constexpr auto kAccountChangeActivityEnable = 2;
constexpr auto kAccountChangeActivityPasswordChange = 3;
constexpr auto kAccountChangeActivityPasswordReset = 4;
constexpr auto kAccountChangeActivityDisable = 5;
constexpr auto kAccountChangeActivityDelete = 6;
constexpr auto kAccountChangeActivityAttachPolicy = 7;
constexpr auto kAccountChangeActivityDetachPolicy = 8;
constexpr auto kAccountChangeActivityLock = 9;
constexpr auto kAccountChangeActivityOther = 99;

constexpr auto kAPIActivityUnknown = 0;
constexpr auto kAPIActivityCreate = 1;
constexpr auto kAPIActivityRead = 2;
constexpr auto kAPIActivityUpdate = 3;
constexpr auto kAPIActivityDelete = 4;
constexpr auto kAPIActivityOther = 99;

constexpr auto kDeviceConfigStateUnknown = 0;
constexpr auto kDeviceConfigStateLog = 1;
constexpr auto kDeviceConfigStateCollect = 2;
constexpr auto kDeviceConfigStateOther = 99;

// severityId
constexpr auto kSeverityUnknown = 0;
constexpr auto kSeverityInformational = 1;
constexpr auto kSeverityLow = 2;
constexpr auto kSeverityMedium = 3;
constexpr auto kSeverityHigh = 4;
constexpr auto kSeverityCritical = 5;
constexpr auto kSeverityFatal = 6;
constexpr auto kSeverityOther = 99;

// StatusId
constexpr auto kStatusUnknown = 0;
constexpr auto kStatusSuccess = 1;
constexpr auto kStatusFailure = 2;
constexpr auto kStatusOther = 99;

inline int fromMongoStatus(Status status) {
    return status.isOK() ? kStatusSuccess : kStatusFailure;
}

/* User.typeId */
constexpr auto kUserTypeIdUnknown = 0;
constexpr auto kUserTypeIdRegularUser = 1;
constexpr auto kUserTypeIdAdminUser = 1;
constexpr auto kUserTypeIdSystemUser = 3;
constexpr auto kUserTypeIdOther = 99;

// Common field names
constexpr auto kATypeFieldName = "atype"_sd;
constexpr auto kUnmappedFieldName = "unmapped"_sd;

}  // namespace mongo::audit::ocsf
