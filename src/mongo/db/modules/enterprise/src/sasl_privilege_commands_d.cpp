/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include <string>

#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/principal.h"
#include "mongo/db/client.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespacestring.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

    class CmdAcquirePrivilege : public Command {
    public:
        CmdAcquirePrivilege() : Command("acquirePrivilege", false, "acquireprivilege") {}
        virtual LockType locktype() const { return NONE; }
        virtual bool requiresAuth() { return false; }
        virtual bool logTheOp() { return false; }
        virtual bool slaveOk() const { return true; }
        virtual void help( stringstream& help ) const {
            help << "Acquires privileges for external authentication principals";
        }
        // Return true if input is valid.  If input is not valid, returns false and writes error
        // description to errmsg
        bool validateInput(BSONObj& cmdObj, std::string& errmsg) {
            if (!cmdObj.hasField("principal")) {
                errmsg = "Must specify principal name";
                return false;
            }
            if (!cmdObj.hasField("resource")) {
                errmsg = "Must specify resource name";
                return false;
            }

            // TODO: remove this check once we have collection-level access control.
            if (NamespaceString::validCollectionName(cmdObj["resource"].str().c_str())) {
                errmsg = "Resource name must be a database, not a collection";
                return false;
            }

            if (!cmdObj.hasField("actions")) {
                errmsg = "Must provide actions to acquire privileges on";
                return false;
            }
            if (cmdObj["actions"].type() != Array) {
                errmsg = "Actions field must be an array";
                return false;
            }

            BSONObjIterator it(cmdObj["actions"].Obj());
            std::string action = it.next().String();
            if (it.more()) {
                errmsg = "actions array must only contain one element for now.  "
                        "Must be one of oldRead or oldWrite";
                return false;
            }
            if (action != "oldWrite" && action != "oldRead") {
                errmsg = "Unsupported action.  The only supported actions currently are "
                        "oldRead and oldWrite";
                return false;
            }
            return true;
        }
        bool run(const std::string& dbname,
                 BSONObj& cmdObj,
                 int,
                 std::string& errmsg,
                 BSONObjBuilder& result,
                 bool fromRepl) {

            if (!validateInput(cmdObj, errmsg)) {
                return false;
            }

            std::string principalName = cmdObj["principal"].str();
            std::string resource = cmdObj["resource"].str();

            // If userSource isn't provided, default to using the database the command was run on.
            std::string userSource = cmdObj.hasField("userSource") ?
                    cmdObj["userSource"].str() : dbname;


            ClientBasic* client = ClientBasic::getCurrent();
            AuthorizationManager* authorizationManager = client->getAuthorizationManager();
            Principal* principal = authorizationManager->lookupPrincipal(principalName, userSource);

            if (!principal) {
                errmsg = "No authenticated principal found with name: " + principalName +
                        " from source: " + userSource;
                return false;
            }

            std::string principalDb;
            std::string principalUsername;
            if (str::splitOn(principalName, '$', principalDb, principalUsername)) {
                // We're doing our own authentication against a principal defined in mongo.
                if (principalDb != resource) {
                    errmsg = "Resource name not the same as database component of principal name";
                    return false;
                } else {
                    // Make sure the principalName matches the name that will be in system.users
                    principalName = principalUsername;
                }
            }

            BSONObj privilegeDocument;
            Status status = authorizationManager->getPrivilegeDocument(resource,
                                                                       principalName,
                                                                       &privilegeDocument);
            if (status != Status::OK()) {
                errmsg = "Problem fetching privilege document: " + status.reason();
                return false;
            }

            status = authorizationManager->acquirePrivilegesFromPrivilegeDocument(
                    userSource, principal, privilegeDocument);

            if (status != Status::OK()) {
                errmsg = "Problem acquiring privileges: " + status.reason();
                return false;
            }

            // TODO: Everything from here down will need to be changed for new-style privilege docs
            bool readOnly = privilegeDocument["readOnly"].trueValue();
            std::string action = BSONObjIterator(cmdObj["actions"].Obj()).next().String();
            if (action == "oldWrite") {
                if (readOnly) {
                    errmsg = "Unauthorized: trying to acquire write privilege as read-only user";
                    return false;
                }
                client->getAuthenticationInfo()->authorize(resource, principalName);
            } else { // Action must be oldRead - already verified in validateInput()
                client->getAuthenticationInfo()->authorizeReadOnly(resource, principalName);
            }

            return true;
        }
    } cmdAcquirePrivilege;

}  // namespace
}  // namespace mongo
