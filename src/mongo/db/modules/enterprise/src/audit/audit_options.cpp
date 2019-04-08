/*
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_options.h"

#include <boost/filesystem.hpp>

#include "audit_event.h"
#include "audit_manager.h"
#include "audit_manager_global.h"
#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/str.h"

namespace moe = mongo::optionenvironment;

namespace mongo {
namespace audit {

AuditGlobalParams auditGlobalParams;

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

MONGO_STARTUP_OPTIONS_STORE(AuditOptions)(InitializerContext* context) {
    const auto& params = moe::startupOptionsParsed;

    if (params.count("auditLog.destination")) {
        auditGlobalParams.enabled = true;
        std::string auditDestination = params["auditLog.destination"].as<std::string>();
        if (auditDestination == "file") {
            if (params.count("auditLog.format")) {
                std::string auditFormat = params["auditLog.format"].as<std::string>();
                if (auditFormat == "JSON") {
                    auditGlobalParams.auditFormat = AuditFormatJsonFile;
                } else if (auditFormat == "BSON") {
                    auditGlobalParams.auditFormat = AuditFormatBsonFile;
                } else {
                    return Status(ErrorCodes::BadValue, "Invalid value for auditLog.format");
                }
            } else {
                return Status(ErrorCodes::BadValue,
                              "auditLog.format must be specified when "
                              "auditLog.destination is to a file");
            }
            if (!params.count("auditLog.path")) {
                return Status(ErrorCodes::BadValue,
                              "auditLog.path must be specified when "
                              "auditLog.destination is to a file");
            }

#ifdef _WIN32
            if (params.count("install") || params.count("reinstall")) {
                if (params.count("auditLog.path") &&
                    !boost::filesystem::path(params["auditLog.path"].as<std::string>())
                         .is_absolute()) {
                    return Status(
                        ErrorCodes::BadValue,
                        "auditLog.path requires an absolute file path with Windows services");
                }
            }
#endif

            auditGlobalParams.auditPath = params["auditLog.path"].as<std::string>();
        } else {
            if (params.count("auditLog.format") || params.count("auditLog.path")) {
                return Status(ErrorCodes::BadValue,
                              "auditLog.format and auditLog.path are only allowed when "
                              "auditLog.destination is 'file'");
            }
            if (auditDestination == "syslog") {
#ifdef _WIN32
                return Status(ErrorCodes::BadValue, "syslog not available on Windows");
#else
                auditGlobalParams.auditFormat = AuditFormatSyslog;
#endif  // ifdef _WIN32
            } else if (auditDestination == "console") {
                auditGlobalParams.auditFormat = AuditFormatConsole;
            } else {
                return Status(ErrorCodes::BadValue, "invalid auditLog destination");
            }
        }
    }

    if (params.count("auditLog.filter")) {
        try {
            auditGlobalParams.auditFilter = fromjson(params["auditLog.filter"].as<std::string>());
        } catch (const DBException& e) {
            return Status(ErrorCodes::BadValue, str::stream() << "bad auditFilter:" << e.what());
        }
    }

    return Status::OK();
}

}  // namespace audit
}  // namespace mongo
