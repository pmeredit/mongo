/*
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_options.h"

#include "audit/audit_options_gen.h"
#include "audit_manager.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/logv2/log.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

namespace mongo {
namespace audit {

Status validateAuditLogDestination(const std::string& strDest) {
    StringData dest(strDest);
    if (!dest.equalCaseInsensitive("console"_sd) && !dest.equalCaseInsensitive("syslog"_sd) &&
        !dest.equalCaseInsensitive("file"_sd) &&
        !((getTestCommandsEnabled() && dest.equalCaseInsensitive("mock")))) {
        return {ErrorCodes::BadValue,
                "auditDestination must be one of 'console', 'syslog', or 'file'"};
    }
    return Status::OK();
}

Status validateAuditLogFormat(const std::string& strFormat) {
    StringData format(strFormat);
    if (!format.equalCaseInsensitive("BSON"_sd) && !format.equalCaseInsensitive("JSON"_sd)) {
        return {ErrorCodes::BadValue, "auditFormat must be one of 'BSON' or 'JSON'"};
    }
    return Status::OK();
}

Status validateAuditLogSchema(const std::string& schema) {
    if (auto swSchema = parseAuditSchema(schema); !swSchema.isOK()) {
        return swSchema.getStatus();
    }
    return Status::OK();
}

void AuditAuthorizationSuccessSetParameter::append(OperationContext*,
                                                   BSONObjBuilder* b,
                                                   StringData name,
                                                   const boost::optional<TenantId>&) {
    b->append(name, getGlobalAuditManager()->getAuditAuthorizationSuccess());
}

Status AuditAuthorizationSuccessSetParameter::set(OperationContext* opCtx,
                                                  const BSONElement& value,
                                                  const boost::optional<TenantId>&) try {
    if ((value.type() == Bool) || value.isNumber()) {
        getGlobalAuditManager()->setAuditAuthorizationSuccess(value.trueValue());
        return Status::OK();
    } else {
        return {ErrorCodes::BadValue,
                str::stream() << "auditAuthorizationSuccess expects bool, got "
                              << typeName(value.type())};
    }
} catch (const DBException& ex) {
    return ex.toStatus();
}

Status AuditAuthorizationSuccessSetParameter::setFromString(OperationContext* opCtx,
                                                            StringData value,
                                                            const boost::optional<TenantId>&) try {
    auto* am = getGlobalAuditManager();
    if ((value == "1") || (value == "true")) {
        am->setAuditAuthorizationSuccess(true);
    } else if ((value == "0") || (value == "false")) {
        am->setAuditAuthorizationSuccess(false);
    } else {
        return {ErrorCodes::BadValue,
                str::stream() << "auditAuthorizationSuccess expects bool, got '" << value << "'"};
    }

    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

void AuditConfigParameter::append(OperationContext*,
                                  BSONObjBuilder* b,
                                  StringData name,
                                  const boost::optional<TenantId>&) {
    b->append(name, getGlobalAuditManager()->getAuditConfig().toBSON());
}

Status AuditConfigParameter::set(OperationContext* opCtx,
                                 const BSONElement& newValueElement,
                                 const boost::optional<TenantId>& tenantId) try {
    AuditConfigDocument newDoc =
        AuditConfigDocument::parse(IDLParserContext("auditConfigDocument"), newValueElement.Obj());

    getGlobalAuditManager()->setConfiguration(Client::getCurrent(), newDoc);
    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

Status AuditConfigParameter::validate(OperationContext* opCtx,
                                      const BSONElement& newValueElement,
                                      const boost::optional<TenantId>&) const try {
    AuditConfigDocument newDoc =
        AuditConfigDocument::parse(IDLParserContext("auditConfigDocument"), newValueElement.Obj());
    auto* am = getGlobalAuditManager();
    uassert(ErrorCodes::AuditingNotEnabled, "Auditing is not enabled", am->isEnabled());
    uassert(ErrorCodes::RuntimeAuditConfigurationNotEnabled,
            "Runtime audit configuration has not been enabled",
            am->getRuntimeConfiguration());

    // Validate that the filter is legal.
    am->parseFilter(newDoc.getFilter());
    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

Status AuditConfigParameter::reset(OperationContext* opCtx, const boost::optional<TenantId>&) try {
    getGlobalAuditManager()->resetConfiguration(Client::getCurrent());
    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

LogicalTime AuditConfigParameter::getClusterParameterTime(const boost::optional<TenantId>&) const {
    auto cpt = getGlobalAuditManager()->getAuditConfig().getClusterParameterTime();
    if (!cpt) {
        return LogicalTime::kUninitialized;
    }
    return *cpt;
}

}  // namespace audit
}  // namespace mongo
