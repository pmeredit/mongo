/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_options.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"

namespace mongo {
namespace audit {
namespace {

template <typename Generator>
class AuthzCheckEvent : public AuditEvent {
public:
    AuthzCheckEvent(const AuditEventEnvelope& envelope,
                    const NamespaceString& ns,
                    StringData commandName,
                    bool redactArgs,
                    Generator&& generator)
        : AuditEvent(envelope),
          _ns(ns),
          _commandName(commandName),
          _redactArgs(redactArgs),
          _generator(std::move(generator)) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        builder.append("command", _commandName);
        builder.append("ns", _ns.ns());
        if (!_redactArgs) {
            BSONObjBuilder argsBuilder(builder.subobjStart("args"));
            _generator(argsBuilder);
        }
        return builder;
    }

    const NamespaceString& _ns;
    StringData _commandName;
    bool _redactArgs;
    const Generator _generator;
};

/**
 * Predicate that determines whether or not an authorization check should
 * be logged, based on global state and the "result" of the authorization
 * check.
 */
bool _shouldLogAuthzCheck(ErrorCodes::Error result) {
    if (!getGlobalAuditManager()->enabled) {
        return false;
    }

    if (auditGlobalParams.auditAuthorizationSuccess.load()) {
        return true;
    }
    if (result != ErrorCodes::OK) {
        return true;
    }

    return false;
}

template <typename G>
void _logAuthzCheck(Client* client,
                    const NamespaceString& ns,
                    StringData commandName,
                    bool redactArgs,
                    G&& generator,
                    ErrorCodes::Error result) {
    AuthzCheckEvent<G> event(makeEnvelope(client, ActionType::authCheck, result),
                             ns,
                             commandName,
                             redactArgs,
                             std::move(generator));

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace

void logCommandAuthzCheck(Client* client,
                          const OpMsgRequest& cmdObj,
                          const CommandInterface& command,
                          ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;


    auto cmdObjEventBuilder = [&](BSONObjBuilder& builder) {
        StringData sensitiveField = command.sensitiveFieldName();

        for (const BSONElement& element : cmdObj.body) {
            if (!sensitiveField.empty() && sensitiveField == element.fieldNameStringData()) {
                builder.append(sensitiveField, "xxx"_sd);
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

    _logAuthzCheck(client,
                   command.ns(),
                   command.getName(),
                   command.redactArgs(),
                   std::move(cmdObjEventBuilder),
                   result);
}

void logDeleteAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& pattern,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    _logAuthzCheck(client,
                   ns,
                   "delete",
                   false,
                   [&](BSONObjBuilder& builder) {
                       builder.append("delete", ns.coll());
                       {
                           BSONArrayBuilder deletes(builder.subarrayStart("deletes"));
                           BSONObjBuilder deleteObj(deletes.subobjStart());
                           deleteObj.append("q", pattern);
                       }
                   },
                   result);
}

void logGetMoreAuthzCheck(Client* client,
                          const NamespaceString& ns,
                          long long cursorId,
                          ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    _logAuthzCheck(client,
                   ns,
                   "getMore",
                   false,
                   [&](BSONObjBuilder& builder) {
                       builder.append("getMore", ns.coll());
                       builder.append("cursorId", cursorId);
                   },
                   result);
}

void logInsertAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& insertedObj,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    _logAuthzCheck(client,
                   ns,
                   "insert",
                   false,
                   [&](BSONObjBuilder& builder) {
                       builder.append("insert", ns.coll());
                       {
                           BSONArrayBuilder documents(builder.subarrayStart("documents"));
                           documents.append(insertedObj);
                       }
                   },
                   result);
}

void logKillCursorsAuthzCheck(Client* client,
                              const NamespaceString& ns,
                              long long cursorId,
                              ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    _logAuthzCheck(client,
                   ns,
                   "killCursors",
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
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    _logAuthzCheck(client,
                   ns,
                   "find",
                   false,
                   [&](BSONObjBuilder& builder) {
                       builder.append("find", ns.coll());
                       builder.append("q", query);
                   },
                   result);
}

void logUpdateAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& query,
                         const BSONObj& updateObj,
                         bool isUpsert,
                         bool isMulti,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    _logAuthzCheck(client,
                   ns,
                   "update",
                   false,
                   [&](BSONObjBuilder& builder) {
                       builder.append("update", ns.coll());
                       {
                           BSONArrayBuilder updates(builder.subarrayStart("updates"));
                           BSONObjBuilder update(updates.subobjStart());
                           update.append("q", query);
                           update.append("u", updateObj);
                           update.append("upsert", isUpsert);
                           update.append("multi", isMulti);
                       }
                   },
                   result);
}

}  // namespace audit
}  // namespace mongo
