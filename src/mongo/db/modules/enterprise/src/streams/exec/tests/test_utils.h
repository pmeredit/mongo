#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/context.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

class MetricManager;

std::unique_ptr<Context> getTestContext(mongo::ServiceContext* svcCtx,
                                        MetricManager* metricManager);

mongo::BSONObj getTestLogSinkSpec();

mongo::BSONObj getTestMemorySinkSpec();

mongo::BSONObj getTestSourceSpec();

}  // namespace streams
