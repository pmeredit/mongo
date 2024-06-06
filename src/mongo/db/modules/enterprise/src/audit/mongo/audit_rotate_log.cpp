/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/util/processinfo.h"

namespace mongo {
namespace audit {
void AuditMongo::logRotateLog(Client* client,
                              const Status& logStatus,
                              const std::vector<Status>& errors,
                              const std::string& suffix) const {
    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kRotateLog,
         [logStatus, suffix, errors](BSONObjBuilder* builder) {
             auto* am = getGlobalAuditManager();
             int pid = static_cast<int>(ProcessId::getCurrent().asInt64());
             std::string targetFile = am->getPath() + suffix;
             builder->appendNumber("pid"_sd, pid);
             {  // os Info
                 BSONObjBuilder osInfoBuilder(builder->subobjStart("osInfo"_sd));
                 osInfoBuilder.append("name"_sd, ProcessInfo::getOsName())
                     .append("version"_sd, ProcessInfo::getOsVersion());
             }
             {  // log status
                 BSONObjBuilder logStatusObjBuilder(builder->subobjStart("logRotationStatus"_sd));
                 logStatusObjBuilder.append("status"_sd, logStatus.toString())
                     .append("rotatedLogPath"_sd, targetFile);
                 {  // log status errors
                     BSONArrayBuilder logRotationArrayBuilder(
                         logStatusObjBuilder.subarrayStart("errors"_sd));
                     for (auto& e : errors) {
                         logRotationArrayBuilder.append(e.toString());
                     }
                 }
             }
         },
         ErrorCodes::OK});
}
}  // namespace audit
}  // namespace mongo
