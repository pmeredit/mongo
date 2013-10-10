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
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"

namespace mongo {
namespace audit {

    AuditGlobalParams auditGlobalParams;

    Status addAuditOptions(moe::OptionSection* options) {

        typedef moe::OptionDescription OD;
        typedef moe::PositionalOptionDescription POD;

        moe::OptionSection auditing_options("Auditing Options");

        Status ret = auditing_options.addOption(OD("auditing", "auditing", moe::Switch,
                    "activate auditing", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = auditing_options.addOption(OD("auditpath", "auditpath", moe::String,
                    "full filespec for audit log", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = auditing_options.addOption(OD("auditformat", "auditformat", moe::String,
                    "text or bson", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = auditing_options.addOption(OD("auditfilter", "auditfilter", moe::String,
                    "filter spec to screen audit records", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = options->addSection(auditing_options);
        if (!ret.isOK()) {
            log() << "Failed to add auditing option section: " << ret.toString();
            return ret;
        }

        return Status::OK();
    }

    Status storeAuditOptions(const moe::Environment& params,
                             const std::vector<std::string>& args) {
        if (params.count("auditing")) {
            auditGlobalParams.enabled = true;
        }

        if (params.count("auditformat")) {
            std::string auditFormatStr = params["auditformat"].as<std::string>();
            if (auditFormatStr == "text") {
                auditGlobalParams.auditFormat = AuditFormatText;
            }
            else if (auditFormatStr == "bson") {
                auditGlobalParams.auditFormat = AuditFormatBson;
            }
            else {
                log() << "invalid auditformat parameter";
                ::_exit(EXIT_FAILURE);
            }
        }
        else {
            // Default to text format if unspecified.
            auditGlobalParams.auditFormat = AuditFormatText;
        }

        if (params.count("auditfilter")) {
            try {
                auditGlobalParams.auditFilter = fromjson(params["auditfilter"].as<std::string>());
            }
            catch (const MsgAssertionException& e) {
                log() << "problem with auditfilter param: " << e.what();
                return Status(ErrorCodes::BadValue, e.what());
            }
        }

        if (params.count("auditpath")) {
            auditGlobalParams.auditPath = params["auditpath"].as<std::string>();
        }
        return Status::OK();
    }

    MONGO_MODULE_STARTUP_OPTIONS_REGISTER(AuditOptions)(InitializerContext* context) {
        return addAuditOptions(&serverOptions);
    }

    MONGO_STARTUP_OPTIONS_STORE(AuditOptions)(InitializerContext* context) {
        return storeAuditOptions(serverParsedOptions, context->args());
    }

    MONGO_INITIALIZER_WITH_PREREQUISITES(InitializeGlobalAuditManager, ("CreateAuditManager"))
                                        (InitializerContext* context) {
        audit::getGlobalAuditManager()->enabled = auditGlobalParams.enabled;

        if (auditGlobalParams.enabled) {
            StatusWithMatchExpression parseResult =
                MatchExpressionParser::parse(auditGlobalParams.auditFilter);
            if (!parseResult.isOK()) {
                return Status(ErrorCodes::BadValue, "failed to parse auditfilter");
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
