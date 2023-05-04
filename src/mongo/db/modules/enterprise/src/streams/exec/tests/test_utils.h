#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/context.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

std::unique_ptr<Context> getTestContext(mongo::ServiceContext* svcCtx = nullptr);

mongo::BSONObj getTestLogSinkSpec();

mongo::BSONObj getTestMemorySinkSpec();

mongo::BSONObj getTestSourceSpec();

}  // namespace streams
