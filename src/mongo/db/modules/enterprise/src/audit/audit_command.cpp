/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

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
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/util/base64.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/sequence_util.h"
#include "mongo/util/stringutils.h"

namespace mongo {

class CmdLogApplicationMessage final : public ErrmsgCommandDeprecated {
public:
    CmdLogApplicationMessage();
    virtual ~CmdLogApplicationMessage();

    Status checkAuthForCommand(Client* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) const override {
        AuthorizationSession* authzSession = AuthorizationSession::get(client);

        if (!authzSession->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                            ActionType::applicationMessage)) {
            return Status(ErrorCodes::Unauthorized,
                          str::stream() << "Not authorized to send custom message to auditlog");
        }
        return Status::OK();
    }

    bool errmsgRun(OperationContext* opCtx,
                   const std::string& db,
                   const BSONObj& cmdObj,
                   std::string& errmsg,
                   BSONObjBuilder& result) override;

    std::string help() const override {
        return "Insert a custom message into the audit log";
    }

    virtual bool isWriteCommandForConfigServer() const {
        return false;
    }
    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }
    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }
};

CmdLogApplicationMessage cmdLogApplicationMessage;

CmdLogApplicationMessage::CmdLogApplicationMessage()
    : ErrmsgCommandDeprecated("logApplicationMessage") {}
CmdLogApplicationMessage::~CmdLogApplicationMessage() {}

bool CmdLogApplicationMessage::errmsgRun(OperationContext* opCtx,
                                         const std::string& db,
                                         const BSONObj& cmdObj,
                                         std::string& errmsg,
                                         BSONObjBuilder& result) {
    Client* client = Client::getCurrent();

    if (cmdObj.hasField("logApplicationMessage")) {
        if (cmdObj["logApplicationMessage"].type() == String) {
            audit::logApplicationMessage(client, cmdObj["logApplicationMessage"].valuestrsafe());
        } else {
            errmsg = "logApplicationMessage takes a string as its only argument";
            return false;
        }
    } else {
        errmsg = "logApplicationMessage missing eponymous field";
        return false;
    }

    return true;
}
}
