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
                    const mutablebson::Document* cmdObj)
        : AuditEvent(envelope), _ns(ns), _cmdObj(cmdObj) {}

private:
    BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const final {
        const auto& cmdElt = _cmdObj->root().leftChild();
        builder.append("command", (!cmdElt.ok() ? StringData("Error") : cmdElt.getFieldName()));
        builder.append("ns", _ns.ns());
        BSONObjBuilder argsBuilder(builder.subobjStart("args"));
        _cmdObj->writeTo(&argsBuilder);
        argsBuilder.done();
        return builder;
    }

    NamespaceString _ns;
    const mutablebson::Document* _cmdObj;
};

/**
 * Predicate that determines whether or not an authorization check should
 * be logged, based on global state and the "result" of the authorization
 * check.
 */
static bool _shouldLogAuthzCheck(ErrorCodes::Error result) {
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

static void _logAuthzCheck(Client* client,
                           const NamespaceString& ns,
                           const mutablebson::Document& cmdObj,
                           ErrorCodes::Error result) {
    AuthzCheckEvent event(makeEnvelope(client, ActionType::authCheck, result), ns, &cmdObj);

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        uassertStatusOK(getGlobalAuditLogDomain()->append(event));
    }
}

}  // namespace
}  // namespace audit

void audit::logCommandAuthzCheck(Client* client,
                                 const OpMsgRequest& cmdObj,
                                 CommandInterface* command,
                                 ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mutablebson::Document cmdToLog(cmdObj.body, mutablebson::Document::kInPlaceDisabled);
    for (auto&& seq : cmdObj.sequences) {
        auto array = cmdToLog.makeElementArray(seq.name);
        for (auto&& obj : seq.objs) {
            // Names for array elements are ignored.
            uassertStatusOK(array.appendObject(StringData(), obj));
        }
        uassertStatusOK(cmdToLog.root().pushBack(array));
    }

    command->redactForLogging(&cmdToLog);

    _logAuthzCheck(client,
                   NamespaceString(command->parseNs(cmdObj.getDatabase().toString(), cmdObj.body)),
                   cmdToLog,
                   result);
}

void audit::logDeleteAuthzCheck(Client* client,
                                const NamespaceString& ns,
                                const BSONObj& pattern,
                                ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassertStatusOK(4054, cmdObj.root().appendString("delete", ns.coll()));
    auto deleteListElt = cmdObj.makeElementArray("deletes");
    auto deleteElt = cmdObj.makeElementObject(StringData());
    fassertStatusOK(4055, deleteElt.appendObject("q", pattern));
    fassertStatusOK(4056, deleteListElt.pushBack(deleteElt));
    fassertStatusOK(4057, cmdObj.root().pushBack(deleteListElt));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void audit::logGetMoreAuthzCheck(Client* client,
                                 const NamespaceString& ns,
                                 long long cursorId,
                                 ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassertStatusOK(4058, cmdObj.root().appendString("getMore", ns.coll()));
    fassertStatusOK(4059, cmdObj.root().appendLong("cursorId", cursorId));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void audit::logInsertAuthzCheck(Client* client,
                                const NamespaceString& ns,
                                const BSONObj& insertedObj,
                                ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassertStatusOK(4060, cmdObj.root().appendString("insert", ns.coll()));
    auto docsListElt = cmdObj.makeElementArray("documents");
    fassertStatusOK(4061, cmdObj.root().pushBack(docsListElt));
    fassertStatusOK(4062, docsListElt.appendObject(StringData(), insertedObj));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void audit::logKillCursorsAuthzCheck(Client* client,
                                     const NamespaceString& ns,
                                     long long cursorId,
                                     ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassertStatusOK(4063, cmdObj.root().appendString("killCursors", ns.coll()));
    fassertStatusOK(4064, cmdObj.root().appendLong("cursorId", cursorId));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void audit::logQueryAuthzCheck(Client* client,
                               const NamespaceString& ns,
                               const BSONObj& query,
                               ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result)) {
        return;
    }

    mutablebson::Document cmdObj;
    fassertStatusOK(4065, cmdObj.root().appendString("find", ns.coll()));
    fassertStatusOK(4066, cmdObj.root().appendObject("q", query));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void audit::logUpdateAuthzCheck(Client* client,
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
    fassertStatusOK(4067, cmdObj.root().appendString("update", ns.coll()));
    auto updatesListElt = cmdObj.makeElementArray("updates");
    fassertStatusOK(4068, cmdObj.root().pushBack(updatesListElt));
    auto updateElt = cmdObj.makeElementObject(StringData());
    fassertStatusOK(4069, updatesListElt.pushBack(updateElt));
    fassertStatusOK(4070, updateElt.appendObject("q", query));
    fassertStatusOK(4071, updateElt.appendObject("u", updateObj));
    fassertStatusOK(4072, updateElt.appendBool("upsert", isUpsert));
    fassertStatusOK(4073, updateElt.appendBool("multi", isMulti));
    _logAuthzCheck(client, ns, cmdObj, result);
}

}  // namespace mongo
