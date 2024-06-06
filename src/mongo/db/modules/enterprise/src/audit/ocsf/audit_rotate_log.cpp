/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/platform/basic.h"

#include "audit/audit_log.h"
#include "audit/ocsf/ocsf_audit_events_gen.h"
#include "audit/ocsf/ocsf_process_activity_constants.h"
#include "audit_ocsf.h"
#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"

namespace mongo::audit {

namespace {

constexpr auto kLogPath = "rotatedLogPath"_sd;
constexpr auto kPid = "pid"_sd;

void serializeErrors(BSONObjBuilder* builder, const std::vector<Status>& errors) {
    BSONArrayBuilder enrichment(builder->subarrayStart(kEnrichmentsId));

    for (const auto& error : errors) {
        BSONObjBuilder errorObj(enrichment.subobjStart());
        errorObj.append(kNameId, kStatusId);
        {
            BSONObjBuilder dataObj(errorObj.subobjStart(kDataId));
            error.serializeErrorToBSON(&dataObj);
        }
    }
}

}  // namespace

void AuditOCSF::logRotateLog(Client* client,
                             const Status& logStatus,
                             const std::vector<Status>& errors,
                             const std::string& suffix) const {
    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kSystemActivity,
         ocsf::OCSFEventClass::kProcessActivity,
         kProcessActivityOther,
         ocsf::fromMongoStatus(logStatus),
         [logStatus, suffix, errors](BSONObjBuilder* builder) {
             auto* am = getGlobalAuditManager();
             int pid = static_cast<int>(ProcessId::getCurrent().asInt64());
             std::string targetFile = am->getPath() + suffix;

             AuditOCSF::AuditEventOCSF::_buildProcess(builder);
             AuditOCSF::AuditEventOCSF::_buildDevice(builder);

             builder->append(kStatusId, ocsf::fromMongoStatus(logStatus));

             {
                 BSONArrayBuilder observables(builder->subarrayStart(kObservablesId));
                 {
                     BSONObjBuilder observable(observables.subobjStart());
                     observable.append(kNameId, kLogPath);
                     observable.append(kTypeId, kTypeFileName);
                     observable.append(kValueId, targetFile);
                 }

                 {
                     BSONObjBuilder observable(observables.subobjStart());
                     observable.append(kNameId, kPid);
                     observable.append(kTypeId, kTypeOther);
                     observable.append(kValueId, pid);
                 }
             }

             serializeErrors(builder, errors);
         },
         ErrorCodes::OK});
}

}  // namespace mongo::audit
