/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/audit_options.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"

namespace mongo::audit {
namespace {

constexpr auto kInsertCommandName = "insert"_sd;
constexpr auto kUpdateCommandName = "update"_sd;
constexpr auto kDeleteCommandName = "delete"_sd;
constexpr auto kFindCommandName = "find"_sd;
constexpr auto kKillCursorsCommandName = "killCursors"_sd;

constexpr auto kOperationFieldName = "operation"_sd;
constexpr auto kAPIFieldName = "api"_sd;
constexpr auto kRequestFieldName = "request"_sd;
constexpr auto kResponseFieldName = "response"_sd;
constexpr auto kUIDFieldName = "uid"_sd;
constexpr auto kCodeFieldName = "code"_sd;
constexpr auto kErrorFieldName = "error"_sd;
constexpr auto kArgsFieldName = "args"_sd;

constexpr auto kKillCursorsFieldName = "killCursors";
constexpr auto kCursorIdFieldName = "cursorId";

constexpr auto kRedactionPlaceholder = "xxx"_sd;

bool _shouldLogAuthzCheck(ErrorCodes::Error result) {
    auto* am = getGlobalAuditManager();
    if (!am->isEnabled()) {
        return false;
    }

    return (result != ErrorCodes::OK) || am->getAuditAuthorizationSuccess();
}

int getActivityFromCommand(StringData cmd) {
    if (cmd == kInsertCommandName) {
        return ocsf::kAPIActivityCreate;
    } else if (cmd == kUpdateCommandName) {
        return ocsf::kAPIActivityUpdate;
    } else if (cmd == kDeleteCommandName) {
        return ocsf::kAPIActivityDelete;
    } else if (cmd == kFindCommandName) {
        return ocsf::kAPIActivityRead;
    } else if (cmd == kKillCursorsCommandName) {
        return ocsf::kAPIActivityOther;
    } else {
        return ocsf::kAPIActivityUnknown;
    }
}

template <typename G>
void _tryLogAuthzCheck(Client* client,
                       const NamespaceString& ns,
                       StringData commandName,
                       bool redactArgs,
                       G&& generator,
                       ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    if (client->isInDirectClient()) {
        return;
    }

    tryLogEvent<AuditOCSF::AuditEventOCSF>(
        {client,
         ocsf::OCSFEventCategory::kApplicationActivity,
         ocsf::OCSFEventClass::kAPIActivity,
         getActivityFromCommand(commandName),
         ocsf::kSeverityInformational,
         [&](BSONObjBuilder* builder) {
             {
                 AuditOCSF::AuditEventOCSF::_buildNetwork(client, builder);
                 BSONObjBuilder api(builder->subobjStart(kAPIFieldName));
                 api.append(kOperationFieldName, commandName);

                 {
                     BSONObjBuilder request(api.subobjStart(kRequestFieldName));
                     request.append(
                         kUIDFieldName,
                         NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()));
                 }

                 {
                     BSONObjBuilder response(api.subobjStart(kResponseFieldName));
                     response.append(kCodeFieldName, result);
                     if (result != ErrorCodes::OK) {
                         response.append(kErrorFieldName, ErrorCodes::errorString(result));
                     }
                 }
             }

             if (!redactArgs) {
                 BSONObjBuilder unmapped(builder->subobjStart(ocsf::kUnmappedFieldName));
                 BSONObjBuilder argsBuilder(unmapped.subobjStart(kArgsFieldName));
                 generator(&argsBuilder);
             }
         },
         result});
}

}  // namespace

void AuditOCSF::logCommandAuthzCheck(Client* client,
                                     const OpMsgRequest& cmdObj,
                                     const CommandInterface& command,
                                     ErrorCodes::Error result) const {
    auto cmdObjEventBuilder = [&](BSONObjBuilder* builder) {
        auto sensitiveFields = command.sensitiveFieldNames();

        for (const BSONElement& element : cmdObj.body.redact(BSONObj::RedactLevel::sensitiveOnly)) {
            auto field = element.fieldNameStringData();
            if (!sensitiveFields.empty() && sensitiveFields.find(field) != sensitiveFields.end()) {
                builder->append(field, kRedactionPlaceholder);
            } else {
                builder->append(element);
            }
        }

        for (auto&& seq : cmdObj.sequences) {
            BSONArrayBuilder arrayBuilder(builder->subarrayStart(seq.name));
            for (auto&& obj : seq.objs) {
                arrayBuilder.append(obj);
            }
        }
    };

    _tryLogAuthzCheck(client,
                      command.ns(),
                      command.getName(),
                      command.redactArgs(),
                      std::move(cmdObjEventBuilder),
                      result);
}

void AuditOCSF::logKillCursorsAuthzCheck(Client* client,
                                         const NamespaceString& ns,
                                         long long cursorId,
                                         ErrorCodes::Error result) const {
    _tryLogAuthzCheck(
        client,
        ns,
        kKillCursorsCommandName,
        false,
        [&](BSONObjBuilder* builder) {
            builder->append(kKillCursorsFieldName, ns.coll());
            builder->append(kCursorIdFieldName, cursorId);
        },
        result);
}

}  // namespace mongo::audit
