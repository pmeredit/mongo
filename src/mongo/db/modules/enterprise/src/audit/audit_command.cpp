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
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/authentication_commands.h"
#include "mongo/util/base64.h"
#include "mongo/util/sequence_util.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {

class CmdLogApplicationMessage final : public BasicCommand {
public:
    CmdLogApplicationMessage();
    ~CmdLogApplicationMessage() override;

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const DatabaseName& dbName,
                                 const BSONObj& cmdObj) const override {
        auto* authzSession = AuthorizationSession::get(opCtx->getClient());

        if (!authzSession->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(dbName.tenantId()),
                ActionType::applicationMessage)) {
            return Status(ErrorCodes::Unauthorized,
                          str::stream() << "Not authorized to send custom message to auditlog");
        }
        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const DatabaseName& dbName,
             const BSONObj& cmdObj,
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
    bool allowedWithSecurityToken() const override {
        return true;
    }
};

MONGO_REGISTER_COMMAND(CmdLogApplicationMessage).forShard().forRouter();

CmdLogApplicationMessage::CmdLogApplicationMessage() : BasicCommand("logApplicationMessage") {}
CmdLogApplicationMessage::~CmdLogApplicationMessage() {}

bool CmdLogApplicationMessage::run(OperationContext* opCtx,
                                   const DatabaseName& dbName,
                                   const BSONObj& cmdObj,
                                   BSONObjBuilder& result) {
    Client* client = Client::getCurrent();

    uassert(ErrorCodes::InvalidOptions,
            "logApplicationMessage missing eponymous field",
            cmdObj.hasField("logApplicationMessage"));

    uassert(ErrorCodes::InvalidBSONType,
            "logApplicationMessage takes a string as its only argument",
            cmdObj["logApplicationMessage"].type() == String);

    audit::logApplicationMessage(client, cmdObj.getStringField("logApplicationMessage"));

    return true;
}
}  // namespace mongo
