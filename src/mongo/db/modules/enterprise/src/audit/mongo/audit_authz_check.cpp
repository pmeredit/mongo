/**
 *    Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "audit/audit_event_type.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/audit_options.h"
#include "audit/mongo/audit_mongo.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"

namespace mongo {
namespace audit {
namespace {

/**
 * Predicate that determines whether or not an authorization check should
 * be logged, based on global state and the "result" of the authorization
 * check.
 */
bool _shouldLogAuthzCheck(ErrorCodes::Error result) {
    auto* am = getGlobalAuditManager();
    if (!am->isEnabled()) {
        return false;
    }

    return (result != ErrorCodes::OK) || am->getAuditAuthorizationSuccess();
}

constexpr auto kCommandField = "command"_sd;
constexpr auto kNSField = "ns"_sd;
constexpr auto kArgsField = "args"_sd;
constexpr auto kKillCursorsCommand = "killCursors"_sd;

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

    tryLogEvent<AuditMongo::AuditEventMongo>(
        {client,
         AuditEventType::kAuthCheck,
         [&](BSONObjBuilder* builder) {
             builder->append(kCommandField, commandName);
             builder->append(
                 kNSField,
                 NamespaceStringUtil::serialize(ns, SerializationContext::stateDefault()));
             if (!redactArgs) {
                 BSONObjBuilder argsBuilder(builder->subobjStart(kArgsField));
                 generator(argsBuilder);
             }
         },
         result});
}

}  // namespace

void AuditMongo::logCommandAuthzCheck(Client* client,
                                      const OpMsgRequest& cmdObj,
                                      const CommandInterface& command,
                                      ErrorCodes::Error result) const {
    auto cmdObjEventBuilder = [&](BSONObjBuilder& builder) {
        auto sensitiveFields = command.sensitiveFieldNames();

        for (const BSONElement& element : cmdObj.body.redact(BSONObj::RedactLevel::sensitiveOnly)) {
            auto field = element.fieldNameStringData();
            if (!sensitiveFields.empty() && sensitiveFields.find(field) != sensitiveFields.end()) {
                builder.append(field, "xxx"_sd);
            } else {
                builder.append(element);
            }
        }

        for (auto&& seq : cmdObj.sequences) {
            BSONArrayBuilder arrayBuilder(builder.subarrayStart(seq.name));
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

void AuditMongo::logKillCursorsAuthzCheck(Client* client,
                                          const NamespaceString& ns,
                                          long long cursorId,
                                          ErrorCodes::Error result) const {
    _tryLogAuthzCheck(
        client,
        ns,
        kKillCursorsCommand,
        false,
        [&](BSONObjBuilder& builder) {
            builder.append("killCursors", ns.coll());
            builder.append("cursorId", cursorId);
        },
        result);
}

}  // namespace audit
}  // namespace mongo
