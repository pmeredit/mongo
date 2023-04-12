#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/context.h"

namespace streams {

std::unique_ptr<Context> getTestContext();

mongo::BSONObj getTestLogSinkSpec();

mongo::BSONObj getTestMemorySinkSpec();

mongo::BSONObj getTestSourceSpec();

}  // namespace streams
