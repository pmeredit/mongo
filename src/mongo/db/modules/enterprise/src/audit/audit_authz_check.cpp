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
#include "mongo/bson/mutable/document.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/namespace_string.h"

namespace mongo {
namespace audit {
namespace {

class AuthzCheckEvent : public AuditEvent {
public:
    AuthzCheckEvent(const AuditEventEnvelope& envelope,
                    const NamespaceString& ns,
                    const mutablebson::Document* cmdObj,
                    bool redactArgs)
        : AuditEvent(envelope), _ns(ns), _cmdObj(cmdObj), _redactArgs(redactArgs) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        const auto& cmdElt = _cmdObj->root().leftChild();
        builder.append("command", (!cmdElt.ok() ? StringData("Error") : cmdElt.getFieldName()));
        builder.append("ns", _ns.ns());
        if (!_redactArgs) {
            BSONObjBuilder argsBuilder(builder.subobjStart("args"));
            _cmdObj->writeTo(&argsBuilder);
            argsBuilder.done();
        }
        return builder;
    }

    NamespaceString _ns;
    const mutablebson::Document* _cmdObj;
    bool _redactArgs;
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

void _logAuthzCheck(Client* client,
                    const NamespaceString& ns,
                    const mutablebson::Document& cmdObj,
                    ErrorCodes::Error result,
                    bool redactArgs = false) {
    AuthzCheckEvent event(
        makeEnvelope(client, ActionType::authCheck, result), ns, &cmdObj, redactArgs);

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

    mutablebson::Document cmdToLog(cmdObj.body, mutablebson::Document::kInPlaceDisabled);

    bool mustRedactArgs = command.redactArgs();

    if (!mustRedactArgs) {
        for (auto&& seq : cmdObj.sequences) {
            auto array = cmdToLog.makeElementArray(seq.name);
            for (auto&& obj : seq.objs) {
                // Names for array elements are ignored.
                uassertStatusOK(array.appendObject(StringData(), obj));
            }
            uassertStatusOK(cmdToLog.root().pushBack(array));
        }
    }
    command.redactForLogging(&cmdToLog);

    _logAuthzCheck(client, command.ns(), cmdToLog, result, mustRedactArgs);
}

void logDeleteAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& pattern,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassert(4054, cmdObj.root().appendString("delete", ns.coll()));
    auto deleteListElt = cmdObj.makeElementArray("deletes");
    auto deleteElt = cmdObj.makeElementObject(StringData());
    fassert(4055, deleteElt.appendObject("q", pattern));
    fassert(4056, deleteListElt.pushBack(deleteElt));
    fassert(4057, cmdObj.root().pushBack(deleteListElt));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logGetMoreAuthzCheck(Client* client,
                          const NamespaceString& ns,
                          long long cursorId,
                          ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassert(4058, cmdObj.root().appendString("getMore", ns.coll()));
    fassert(4059, cmdObj.root().appendLong("cursorId", cursorId));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logInsertAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& insertedObj,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassert(4060, cmdObj.root().appendString("insert", ns.coll()));
    auto docsListElt = cmdObj.makeElementArray("documents");
    fassert(4061, cmdObj.root().pushBack(docsListElt));
    fassert(4062, docsListElt.appendObject(StringData(), insertedObj));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logKillCursorsAuthzCheck(Client* client,
                              const NamespaceString& ns,
                              long long cursorId,
                              ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassert(4063, cmdObj.root().appendString("killCursors", ns.coll()));
    fassert(4064, cmdObj.root().appendLong("cursorId", cursorId));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logQueryAuthzCheck(Client* client,
                        const NamespaceString& ns,
                        const BSONObj& query,
                        ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassert(4065, cmdObj.root().appendString("find", ns.coll()));
    fassert(4066, cmdObj.root().appendObject("q", query));
    _logAuthzCheck(client, ns, cmdObj, result);
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

    mutablebson::Document cmdObj;
    fassert(4067, cmdObj.root().appendString("update", ns.coll()));
    auto updatesListElt = cmdObj.makeElementArray("updates");
    fassert(4068, cmdObj.root().pushBack(updatesListElt));
    auto updateElt = cmdObj.makeElementObject(StringData());
    fassert(4069, updatesListElt.pushBack(updateElt));
    fassert(4070, updateElt.appendObject("q", query));
    fassert(4071, updateElt.appendObject("u", updateObj));
    fassert(4072, updateElt.appendBool("upsert", isUpsert));
    fassert(4073, updateElt.appendBool("multi", isMulti));
    _logAuthzCheck(client, ns, cmdObj, result);
}

}  // namespace audit
}  // namespace mongo
