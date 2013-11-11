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
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace audit {

    AuditGlobalParams auditGlobalParams;

    Status addAuditOptions(moe::OptionSection* options) {

        moe::OptionSection auditingOptions("Auditing Options");

        auditingOptions.addOptionChaining("auditLog", "auditLog", moe::String,
                                          "turn on auditing and specify output for log: "
                                          "jsonfile, bsonfile, syslog, console");

        auditingOptions.addOptionChaining("auditPath", "auditPath", moe::String,
                                          "full filespec for audit log file");

        auditingOptions.addOptionChaining("auditFilter", "auditFilter", moe::String,
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
        if (params.count("auditLog")) {
            auditGlobalParams.enabled = true;
            std::string auditFormatStr = params["auditLog"].as<std::string>();
            if (auditFormatStr == "jsonfile") {
                auditGlobalParams.auditFormat = AuditFormatJsonFile;
                auditGlobalParams.auditPath = params["auditPath"].as<std::string>();
            }
            else if (auditFormatStr == "bsonfile") {
                auditGlobalParams.auditFormat = AuditFormatBsonFile;
                auditGlobalParams.auditPath = params["auditPath"].as<std::string>();
            }
            else if (auditFormatStr == "syslog") {
#ifdef _WIN32
                return Status(ErrorCodes::BadValue, "syslog not available on Windows");
#else
                auditGlobalParams.auditFormat = AuditFormatSyslog;
#endif // ifdef _WIN32
            }
            else if (auditFormatStr == "console") {
                auditGlobalParams.auditFormat = AuditFormatConsole;
            }
            else {
                return Status(ErrorCodes::BadValue, "invalid auditLog parameter");
            }
        }

        if (params.count("auditFilter")) {
            try {
                auditGlobalParams.auditFilter = fromjson(params["auditFilter"].as<std::string>());
            }
            catch (const MsgAssertionException& e) {
                return Status(ErrorCodes::BadValue,
                              mongoutils::str::stream() << "bad auditFilter:" << e.what());
            }
        }

        return Status::OK();
    }

    MONGO_MODULE_STARTUP_OPTIONS_REGISTER(AuditOptions)(InitializerContext* context) {
        return addAuditOptions(&moe::startupOptions);
    }

    MONGO_STARTUP_OPTIONS_STORE(AuditOptions)(InitializerContext* context) {
        return storeAuditOptions(moe::startupOptionsParsed, context->args());
    }

    MONGO_INITIALIZER_WITH_PREREQUISITES(InitializeGlobalAuditManager, ("CreateAuditManager"))
                                        (InitializerContext* context) {
        audit::getGlobalAuditManager()->enabled = auditGlobalParams.enabled;

        if (auditGlobalParams.enabled) {
            StatusWithMatchExpression parseResult =
                MatchExpressionParser::parse(auditGlobalParams.auditFilter);
            if (!parseResult.isOK()) {
                return Status(ErrorCodes::BadValue, "failed to parse auditFilter");
            }
            AuditManager* am = audit::getGlobalAuditManager();
            am->auditFilter = parseResult.getValue();

            am->auditLogPath = auditGlobalParams.auditPath;

            am->auditFormat = auditGlobalParams.auditFormat;
        }

        return Status::OK();
    }
} // namespace audit
} // namespace mongo
