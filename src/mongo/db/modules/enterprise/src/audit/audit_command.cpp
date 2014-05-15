/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include <algorithm>
#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/mongo_authentication_session.h"
#include "mongo/db/client_basic.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/sequence_util.h"
#include "mongo/util/stringutils.h"

namespace mongo {

    class CmdLogApplicationMessage : public Command {
    public:
        CmdLogApplicationMessage();
        virtual ~CmdLogApplicationMessage();

        virtual Status checkAuthForCommand(ClientBasic* client,
                                           const std::string& dbname,
                                           const BSONObj& cmdObj) {
            AuthorizationSession* authzSession = client->getAuthorizationSession();

            if (!authzSession->isAuthorizedForActionsOnResource(
                    ResourcePattern::forClusterResource(), ActionType::applicationMessage)) {
                return Status(ErrorCodes::Unauthorized,
                              str::stream() << "Not authorized to send custom message to auditlog");
            }
            return Status::OK();
        }

        virtual bool run(OperationContext* txn,
                         const std::string& db,
                         BSONObj& cmdObj,
                         int options,
                         std::string& errmsg,
                         BSONObjBuilder& result,
                         bool fromRepl);

        virtual void help(stringstream& help) const;
        virtual bool isWriteCommandForConfigServer() const { return false; }
        virtual bool slaveOk() const { return true; }
    };

    CmdLogApplicationMessage cmdLogApplicationMessage;

    CmdLogApplicationMessage::CmdLogApplicationMessage() : Command("logApplicationMessage") {}
    CmdLogApplicationMessage::~CmdLogApplicationMessage() {}

    void CmdLogApplicationMessage::help(std::stringstream& os) const {
        os << "Insert a custom message into the audit log";
    }

    bool CmdLogApplicationMessage::run(OperationContext* txn,
                                       const std::string& db,
                                       BSONObj& cmdObj,
                                       int options,
                                       std::string& errmsg,
                                       BSONObjBuilder& result,
                                       bool fromRepl) {

        ClientBasic* client = ClientBasic::getCurrent();

        if (cmdObj.hasField("logApplicationMessage")) {
            if (cmdObj["logApplicationMessage"].type() == String) {
                audit::logApplicationMessage(client, cmdObj["logApplicationMessage"].valuestrsafe());
            }
            else {
                errmsg = "logApplicationMessage takes a string as its only argument";
                return false;
            }
        }
        else {
            errmsg = "logApplicationMessage missing eponymous field";
            return false;
        }

        return true;
    }
}
