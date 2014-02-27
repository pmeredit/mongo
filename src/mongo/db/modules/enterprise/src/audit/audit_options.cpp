/*
 *    Copyright (C) 2013 10gen Inc.
 */

#include "audit_options.h"

#include "audit_event.h"
#include "audit_manager.h"
#include "audit_manager_global.h"
#include "mongo/base/init.h"
#include "mongo/db/server_options.h"
#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace audit {

    AuditGlobalParams auditGlobalParams;

    Status addAuditOptions(moe::OptionSection* options) {

        moe::OptionSection auditingOptions("Auditing Options");

        auditingOptions.addOptionChaining("auditLog.format", "auditFormat", moe::String,
                                          "Format of the audit log, if logging to a file.  "
                                          "(BSON/JSON)")
                                         .format("(:?BSON)|(:?JSON)", "(BSON/JSON)");

        auditingOptions.addOptionChaining("auditLog.destination", "auditDestination",
                                          moe::String, "Destination of audit log output.  "
                                          "(console/syslog/file)")
                                         .format("(:?console)|(:?syslog)|(:?file)",
                                                 "(console/syslog/file)");

        auditingOptions.addOptionChaining("auditLog.path", "auditPath", moe::String,
                                          "full filespec for audit log file");

        auditingOptions.addOptionChaining("auditLog.filter", "auditFilter", moe::String,
                                          "filter spec to screen audit records");

        Status ret = options->addSection(auditingOptions);
        if (!ret.isOK()) {
            log() << "Failed to add auditing option section: " << ret.toString();
            return ret;
        }

        return Status::OK();
    }

    Status storeAuditOptions(const moe::Environment& params,
                             const std::vector<std::string>& args) {

        if (params.count("auditLog.destination")) {
            auditGlobalParams.enabled = true;
            std::string auditDestination = params["auditLog.destination"].as<std::string>();
            if (auditDestination == "file") {
                if (params.count("auditLog.format")) {
                    std::string auditFormat = params["auditLog.format"].as<std::string>();
                    if (auditFormat == "JSON") {
                        auditGlobalParams.auditFormat = AuditFormatJsonFile;
                    }
                    else if (auditFormat == "BSON") {
                        auditGlobalParams.auditFormat = AuditFormatBsonFile;
                    }
                    else {
                        return Status(ErrorCodes::BadValue, "Invalid value for auditLog.format");
                    }
                }
                else {
                    return Status(ErrorCodes::BadValue, "auditLog.format must be specified when "
                                                        "auditLog.destination is to a file");
                }
                if (!params.count("auditLog.path")) {
                    return Status(ErrorCodes::BadValue, "auditLog.path must be specified when "
                                                        "auditLog.destination is to a file");
                }
                auditGlobalParams.auditPath = params["auditLog.path"].as<std::string>();
            }
            else {
                if (params.count("auditLog.format")) {
                    return Status(ErrorCodes::BadValue, "auditLog.format not allowed when "
                                                        "auditLog.destination is not to a file");
                }
                if (auditDestination == "syslog") {
#ifdef _WIN32
                    return Status(ErrorCodes::BadValue, "syslog not available on Windows");
#else
                    auditGlobalParams.auditFormat = AuditFormatSyslog;
#endif // ifdef _WIN32
                }
                else if (auditDestination == "console") {
                    auditGlobalParams.auditFormat = AuditFormatConsole;
                }
                else {
                    return Status(ErrorCodes::BadValue, "invalid auditLog destination");
                }
            }
        }

        if (params.count("auditLog.filter")) {
            try {
                auditGlobalParams.auditFilter =
                    fromjson(params["auditLog.filter"].as<std::string>());
            }
            catch (const MsgAssertionException& e) {
                return Status(ErrorCodes::BadValue,
                              mongoutils::str::stream() << "bad auditFilter:" << e.what());
            }
        }

        return Status::OK();
    }

    ExportedServerParameter<bool> AuditAuthzSuccessSetting(ServerParameterSet::getGlobal(),
                                                          "auditAuthzSuccess",
                                                          &auditGlobalParams.auditAuthzSuccess,
                                                          true,  // Change at startup
                                                          true); // Change at runtime

} // namespace audit
} // namespace mongo
