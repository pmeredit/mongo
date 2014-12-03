/**
 *    Copyright (C) 2013 10gen Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "audit_event.h"
#include "audit_log_domain.h"
#include "audit_manager_global.h"
#include "audit_options.h"
#include "audit_private.h"
#include "mongo/base/status.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client_basic.h"
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
            : AuditEvent(envelope), _ns(ns), _cmdObj(cmdObj) {
        }
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
        }
        else {
            os << "Access denied on ";
        }
        os << _ns.ns();
        os << " for " << _cmdObj->toString() << '.';
        return os;
    }

    BSONObjBuilder& AuthzCheckEvent::putParamsBSON(BSONObjBuilder& builder) const {
        mmb::ConstElement cmdElt = _cmdObj->root().leftChild();
        builder.append("command", (!cmdElt.ok() ? StringData("Error") : cmdElt.getFieldName()));
        builder.append("ns", _ns);
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
        if (!getGlobalAuditManager()->enabled) return false;

        if (auditGlobalParams.auditAuthorizationSuccess) return true;
        if (result != ErrorCodes::OK) return true;

        return false;
    }

    static void _logAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            const mmb::Document& cmdObj,
            ErrorCodes::Error result) {

        AuthzCheckEvent event(
                makeEnvelope(client, ActionType::authCheck, result),
                ns,
                &cmdObj);

        if (getGlobalAuditManager()->auditFilter->matches(&event)) {
            getGlobalAuditLogDomain()->append(event);
        }
    }

    void logCommandAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            const BSONObj& cmdObj,
            Command* command,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdToLog(cmdObj, mmb::Document::kInPlaceDisabled);
        command->redactForLogging(&cmdToLog);

        _logAuthzCheck(client, ns, cmdToLog, result);
    }

    void logDeleteAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            const BSONObj& pattern,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendString("delete", ns.coll()));
        mmb::Element deleteListElt = cmdObj.makeElementArray("deletes");
        mmb::Element deleteElt = cmdObj.makeElementObject(StringData());
        fassertStatusOK(deleteElt.appendObject("q", pattern));
        fassertStatusOK(deleteListElt.pushBack(deleteElt));
        fassertStatusOK(cmdObj.root().pushBack(deleteListElt));
        _logAuthzCheck(client, ns, cmdObj, result);
    }

    void logFsyncUnlockAuthzCheck(
            ClientBasic* client,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendInt("fsyncUnlock", 1));
        _logAuthzCheck(client, NamespaceString("admin"), cmdObj, result);
    }

    void logGetMoreAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            long long cursorId,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendString("getMore", ns.coll()));
        fassertStatusOK(cmdObj.root().appendLong("cursorId", cursorId));
        _logAuthzCheck(client, ns, cmdObj, result);
    }

    void logInProgAuthzCheck(
            ClientBasic* client,
            const BSONObj& filter,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendInt("inprog", 1));
        fassertStatusOK(cmdObj.root().appendObject("q", filter));
        _logAuthzCheck(client, NamespaceString("admin"), cmdObj, result);
    }

    void logInsertAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            const BSONObj& insertedObj,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendString("insert", ns.coll()));
        mmb::Element docsListElt = cmdObj.makeElementArray("documents");
        fassertStatusOK(cmdObj.root().pushBack(docsListElt));
        fassertStatusOK(docsListElt.appendObject(StringData(), insertedObj));
        _logAuthzCheck(client, ns, cmdObj, result);
    }

    void logKillCursorsAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            long long cursorId,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendString("killCursors", ns.coll()));
        fassertStatusOK(cmdObj.root().appendLong("cursorId", cursorId));
        _logAuthzCheck(client, ns, cmdObj, result);
    }

    void logKillOpAuthzCheck(
            ClientBasic* client,
            const BSONObj& filter,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendInt("killOp", 1));
        fassertStatusOK(cmdObj.root().appendObject("q", filter));
        _logAuthzCheck(client, NamespaceString("admin"), cmdObj, result);
    }

    void logQueryAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            const BSONObj& query,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendString("find", ns.coll()));
        fassertStatusOK(cmdObj.root().appendObject("q", query));
        _logAuthzCheck(client, ns, cmdObj, result);
    }

    void logUpdateAuthzCheck(
            ClientBasic* client,
            const NamespaceString& ns,
            const BSONObj& query,
            const BSONObj& updateObj,
            bool isUpsert,
            bool isMulti,
            ErrorCodes::Error result) {

        if (!_shouldLogAuthzCheck(result)) return;

        mmb::Document cmdObj;
        fassertStatusOK(cmdObj.root().appendString("update", ns.coll()));
        mmb::Element updatesListElt = cmdObj.makeElementArray("updates");
        fassertStatusOK(cmdObj.root().pushBack(updatesListElt));
        mmb::Element updateElt = cmdObj.makeElementObject(StringData());
        fassertStatusOK(updatesListElt.pushBack(updateElt));
        fassertStatusOK(updateElt.appendObject("q", query));
        fassertStatusOK(updateElt.appendObject("u", updateObj));
        fassertStatusOK(updateElt.appendBool("upsert", isUpsert));
        fassertStatusOK(updateElt.appendBool("multi", isMulti));
        _logAuthzCheck(client, ns, cmdObj, result);
    }

}  // namespace audit
}  // namespace mongo
