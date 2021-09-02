/**
 *    Copyright (C) 2021 MongoDB Inc.
 */

#pragma once

#include <string>

#include "mongo/bson/bsonobj.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace audit {

struct AuditFrame {
    Date_t ts;
    BSONObj event;
};

}  // namespace audit
}  // namespace mongo
