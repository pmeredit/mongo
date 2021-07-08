/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_event_type.h"
#include "audit_log.h"
#include "audit_manager.h"
#include "audit_options.h"
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
constexpr auto kDeleteCommand = "delete"_sd;
constexpr auto kGetMoreCommand = "getMore"_sd;
constexpr auto kInsertCommand = "insert"_sd;
constexpr auto kKillCursorsCommand = "killCursors"_sd;
constexpr auto kFindCommand = "find"_sd;
constexpr auto kUpdateCommand = "update"_sd;

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

    tryLogEvent(client,
                AuditEventType::kAuthCheck,
                [&](BSONObjBuilder* builder) {
                    builder->append(kCommandField, commandName);
                    builder->append(kNSField, ns.ns());
                    if (!redactArgs) {
                        BSONObjBuilder argsBuilder(builder->subobjStart(kArgsField));
                        generator(argsBuilder);
                    }
                },
                result);
}

}  // namespace

void logCommandAuthzCheck(Client* client,
                          const OpMsgRequest& cmdObj,
                          const CommandInterface& command,
                          ErrorCodes::Error result) {
    auto cmdObjEventBuilder = [&](BSONObjBuilder& builder) {
        auto sensitiveFields = command.sensitiveFieldNames();

        for (const BSONElement& element : cmdObj.body) {
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

void logGetMoreAuthzCheck(Client* client,
                          const NamespaceString& ns,
                          long long cursorId,
                          ErrorCodes::Error result) {
    _tryLogAuthzCheck(client,
                      ns,
                      kGetMoreCommand,
                      false,
                      [&](BSONObjBuilder& builder) {
                          builder.append("getMore", ns.coll());
                          builder.append("cursorId", cursorId);
                      },
                      result);
}

void logKillCursorsAuthzCheck(Client* client,
                              const NamespaceString& ns,
                              long long cursorId,
                              ErrorCodes::Error result) {
    _tryLogAuthzCheck(client,
                      ns,
                      kKillCursorsCommand,
                      false,
                      [&](BSONObjBuilder& builder) {
                          builder.append("killCursors", ns.coll());
                          builder.append("cursorId", cursorId);
                      },
                      result);
}

void logQueryAuthzCheck(Client* client,
                        const NamespaceString& ns,
                        const BSONObj& query,
                        ErrorCodes::Error result) {
    _tryLogAuthzCheck(client,
                      ns,
                      kFindCommand,
                      false,
                      [&](BSONObjBuilder& builder) {
                          builder.append("find", ns.coll());
                          builder.append("q", query);
                      },
                      result);
}

}  // namespace audit
}  // namespace mongo
