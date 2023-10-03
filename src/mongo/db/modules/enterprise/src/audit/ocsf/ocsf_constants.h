/**
 *    Copyright (C) 2023 MongoDB Inc.
 */
#pragma once

namespace mongo::audit::ocsf {

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

}  // namespace mongo::audit::ocsf
