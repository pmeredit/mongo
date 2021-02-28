/*
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_options.h"

#include "audit/audit_options_gen.h"
#include "audit_manager.h"

namespace mongo {
namespace audit {

Status validateAuditLogDestination(const std::string& strDest) {
    StringData dest(strDest);
    if (!dest.equalCaseInsensitive("console"_sd) && !dest.equalCaseInsensitive("syslog"_sd) &&
        !dest.equalCaseInsensitive("file"_sd)) {
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

void AuditAuthorizationSuccessSetParameter::append(OperationContext*,
                                                   BSONObjBuilder& b,
                                                   const std::string& name) {
    b.append(name, getGlobalAuditManager()->getAuditAuthorizationSuccess());
}

Status AuditAuthorizationSuccessSetParameter::set(const BSONElement& value) try {
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

Status AuditAuthorizationSuccessSetParameter::setFromString(const std::string& value) try {
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

}  // namespace audit
}  // namespace mongo
