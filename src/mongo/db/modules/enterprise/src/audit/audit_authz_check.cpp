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

namespace mmb = mongo::mutablebson;

class AuthzCheckEvent : public AuditEvent {
public:
    AuthzCheckEvent(const AuditEventEnvelope& envelope,
                    const NamespaceString& ns,
                    const mmb::Document* cmdObj)
        : AuditEvent(envelope), _ns(ns), _cmdObj(cmdObj) {}
    virtual ~AuthzCheckEvent() {}

private:
    virtual std::ostream& putTextDescription(std::ostream& os) const;
    virtual BSONObjBuilder& putParamsBSON(BSONObjBuilder& builder) const;

    NamespaceString _ns;
    const mmb::Document* _cmdObj;
};

std::ostream& AuthzCheckEvent::putTextDescription(std::ostream& os) const {
    if (getResultCode() == ErrorCodes::OK) {
        os << "Access granted on ";
    } else {
        os << "Access denied on ";
    }
    os << _ns.ns();
    os << " for " << _cmdObj->toString() << '.';
    return os;
}

BSONObjBuilder& AuthzCheckEvent::putParamsBSON(BSONObjBuilder& builder) const {
    mmb::ConstElement cmdElt = _cmdObj->root().leftChild();
    builder.append("command", (!cmdElt.ok() ? StringData("Error") : cmdElt.getFieldName()));
    builder.append("ns", _ns.ns());
    BSONObjBuilder argsBuilder(builder.subobjStart("args"));
    _cmdObj->writeTo(&argsBuilder);
    argsBuilder.done();
    return builder;
}

/**
 * Predicate that determines whether or not an authorization check should
 * be logged, based on global state and the "result" of the authorization
 * check.
 */
static bool _shouldLogAuthzCheck(ErrorCodes::Error result) {
    if (!getGlobalAuditManager()->enabled)
        return false;

    if (auditGlobalParams.auditAuthorizationSuccess.load())
        return true;
    if (result != ErrorCodes::OK)
        return true;

    return false;
}

static void _logAuthzCheck(Client* client,
                           const NamespaceString& ns,
                           const mmb::Document& cmdObj,
                           ErrorCodes::Error result) {
    AuthzCheckEvent event(makeEnvelope(client, ActionType::authCheck, result), ns, &cmdObj);

    if (getGlobalAuditManager()->auditFilter->matches(&event)) {
        getGlobalAuditLogDomain()->append(event).transitional_ignore();
    }
}

void logCommandAuthzCheck(Client* client,
                          const std::string& dbname,
                          const BSONObj& cmdObj,
                          CommandInterface* command,
                          ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mmb::Document cmdToLog(cmdObj, mmb::Document::kInPlaceDisabled);
    command->redactForLogging(&cmdToLog);

    _logAuthzCheck(client, NamespaceString(command->parseNs(dbname, cmdObj)), cmdToLog, result);
}

void logDeleteAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& pattern,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mmb::Document cmdObj;
    fassertStatusOK(4054, cmdObj.root().appendString("delete", ns.coll()));
    mmb::Element deleteListElt = cmdObj.makeElementArray("deletes");
    mmb::Element deleteElt = cmdObj.makeElementObject(StringData());
    fassertStatusOK(4055, deleteElt.appendObject("q", pattern));
    fassertStatusOK(4056, deleteListElt.pushBack(deleteElt));
    fassertStatusOK(4057, cmdObj.root().pushBack(deleteListElt));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logGetMoreAuthzCheck(Client* client,
                          const NamespaceString& ns,
                          long long cursorId,
                          ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mmb::Document cmdObj;
    fassertStatusOK(4058, cmdObj.root().appendString("getMore", ns.coll()));
    fassertStatusOK(4059, cmdObj.root().appendLong("cursorId", cursorId));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logInsertAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& insertedObj,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mmb::Document cmdObj;
    fassertStatusOK(4060, cmdObj.root().appendString("insert", ns.coll()));
    mmb::Element docsListElt = cmdObj.makeElementArray("documents");
    fassertStatusOK(4061, cmdObj.root().pushBack(docsListElt));
    fassertStatusOK(4062, docsListElt.appendObject(StringData(), insertedObj));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logKillCursorsAuthzCheck(Client* client,
                              const NamespaceString& ns,
                              long long cursorId,
                              ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mmb::Document cmdObj;
    fassertStatusOK(4063, cmdObj.root().appendString("killCursors", ns.coll()));
    fassertStatusOK(4064, cmdObj.root().appendLong("cursorId", cursorId));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logQueryAuthzCheck(Client* client,
                        const NamespaceString& ns,
                        const BSONObj& query,
                        ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mmb::Document cmdObj;
    fassertStatusOK(4065, cmdObj.root().appendString("find", ns.coll()));
    fassertStatusOK(4066, cmdObj.root().appendObject("q", query));
    _logAuthzCheck(client, ns, cmdObj, result);
}

void logUpdateAuthzCheck(Client* client,
                         const NamespaceString& ns,
                         const BSONObj& query,
                         const BSONObj& updateObj,
                         bool isUpsert,
                         bool isMulti,
                         ErrorCodes::Error result) {
    if (!_shouldLogAuthzCheck(result))
        return;

    mmb::Document cmdObj;
    fassertStatusOK(4067, cmdObj.root().appendString("update", ns.coll()));
    mmb::Element updatesListElt = cmdObj.makeElementArray("updates");
    fassertStatusOK(4068, cmdObj.root().pushBack(updatesListElt));
    mmb::Element updateElt = cmdObj.makeElementObject(StringData());
    fassertStatusOK(4069, updatesListElt.pushBack(updateElt));
    fassertStatusOK(4070, updateElt.appendObject("q", query));
    fassertStatusOK(4071, updateElt.appendObject("u", updateObj));
    fassertStatusOK(4072, updateElt.appendBool("upsert", isUpsert));
    fassertStatusOK(4073, updateElt.appendBool("multi", isMulti));
    _logAuthzCheck(client, ns, cmdObj, result);
}

}  // namespace audit
}  // namespace mongo
