/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <string>

#include "mongo/bson/bsonobj.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace audit {

template <typename T>
struct AuditFrame {
    Date_t ts;
    T payload;

    static constexpr auto kTimestampField = "ts"_sd;
    static constexpr auto kLogField = "log"_sd;
};

using EncryptedAuditFrame = AuditFrame<std::vector<std::uint8_t>>;
using PlainAuditFrame = AuditFrame<BSONObj>;

}  // namespace audit
}  // namespace mongo
