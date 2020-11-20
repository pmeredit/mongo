/**
 *  Copyright (C) 2020 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "audit_event_type.h"

#include <cstdint>
#include <fmt/format.h>
#include <iostream>
#include <map>
#include <set>
#include <string>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/util/static_immortal.h"

namespace mongo {

namespace {

struct AuditEventEntry {
    StringData name;
    AuditEventType event;
};

constexpr std::array kTable{
#define X_(a) AuditEventEntry{#a ""_sd, AuditEventType::a},
    EXPAND_AUDIT_EVENT_TYPE(X_)
#undef X_
};

constexpr bool isTableIndexedProperly() {
    if (kTable.size() != kNumAuditEventTypes)
        return false;
    for (size_t i = 0; i < kTable.size(); ++i)
        if (kTable[i].event != static_cast<AuditEventType>(i))
            return false;
    return true;
}
static_assert(isTableIndexedProperly());

}  // namespace

StatusWith<AuditEventType> parseAuditEventFromString(StringData event) {
    static const StaticImmortal byName = [] {
        std::map<StringData, AuditEventType> m;
        for (auto&& e : kTable)
            m.insert({e.name, e.event});
        return m;
    }();
    if (auto iter = byName->find(event); iter != byName->end()) {
        return iter->second;
    }
    return Status(ErrorCodes::FailedToParse,
                  fmt::format("Unrecognized event privilege string: {}", event));
}

StringData toStringData(AuditEventType a) {
    return kTable[static_cast<size_t>(a)].name;
}

std::string toString(AuditEventType a) {
    return std::string{toStringData(a)};
}

std::ostream& operator<<(std::ostream& os, const AuditEventType& a) {
    return os << toStringData(a);
}

}  // namespace mongo