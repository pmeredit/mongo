#include "streams/exec/context.h"

namespace streams {

mongo::BSONObj Context::toBSON() {
    return BSON("streamProcessorName" << streamName << "streamProcessorId" << streamProcessorId
                                      << "tenantId" << tenantId);
}

}  // namespace streams
