/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include "audit_options.h"

#include "audit_manager.h"
#include "audit_manager_global.h"
#include "audit_event.h"
#include "mongo/base/status.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/module.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/option_description.h"

namespace mongo {

    namespace moe = mongo::optionenvironment;

namespace audit {

    class AuditOptions : public Module {

    public:
        AuditOptions() : Module("auditing"), 
                         _enabled(false),
                         _auditFormat(AuditFormatText) {}

        virtual void init() {
            // Initialize the filter(s).
            // This should move to the MONGO_INITIALIZER once we get rid of Module
            getGlobalAuditManager()->enabled = _enabled;

            if (_enabled) {
                StatusWithMatchExpression parseResult = MatchExpressionParser::parse(_auditfilter);
                if (!parseResult.isOK()) {
                    //return status;
                    fassertFailed(17185);
                }
                getGlobalAuditManager()->auditFilter = parseResult.getValue();
            }
        }

        virtual void shutdown() {}

        void addOptions(moe::OptionSection* options) {
            typedef moe::OptionDescription OD;
            
            moe::OptionSection auditing_options("Auditing Options");
            
            Status ret = auditing_options.addOption(OD("auditing", "auditing", moe::Switch,
                                                       "activate auditing", true));
            if (!ret.isOK()) {
                log() << "Failed to register auditing option: " << ret.toString();
                return;
            }

            ret = auditing_options.addOption(OD("auditpath", "auditpath", moe::String,
                                                "full filespec for audit log", true));
            if (!ret.isOK()) {
                log() << "Failed to register auditpath option: " << ret.toString();
                return;
            }

            ret = auditing_options.addOption(OD("auditformat", "auditformat", moe::String,
                                                "text or bson", true));
            if (!ret.isOK()) {
                log() << "Failed to register auditformat option: " << ret.toString();
                return;
            }
            
            ret = auditing_options.addOption(OD("auditfilter", "auditfilter", moe::String,
                                                "filter spec to screen audit records", true));
            if (!ret.isOK()) {
                log() << "Failed to register auditfilter option: " << ret.toString();
                return;
            }

            ret = options->addSection(auditing_options);
            if (!ret.isOK()) {
                log() << "Failed to add auditing option section: " << ret.toString();
                return;
            }
        } 

        virtual void config(moe::Environment& params) {
            if (params.count("auditing")) {
                _enabled = true;
            }
            if (params.count("auditformat")) {
                std::string auditFormatStr = params["auditformat"].as<std::string>();
                if (auditFormatStr == "text") {
                    _auditFormat = AuditFormatText;
                }
                else if (auditFormatStr == "bson") {
                    _auditFormat = AuditFormatBson;
                }
                else {
                    log() << "invalid auditformat parameter";
                    ::_exit(EXIT_FAILURE);
                }
            }
            if (params.count("auditfilter")) {
                try {
                    _auditfilter = fromjson(params["auditfilter"].as<std::string>());
                }
                catch (const MsgAssertionException& e) {
                    // TODO: This is rather messy.
                    //      Easier when this is in a MONGO_INTIIALIZER
                    log() << "problem with auditfilter param: " << e.what();
                    ::_exit(EXIT_FAILURE);
                }
            }
        }

        bool isEnabled() const {
            return _enabled;
        }

        AuditFormat getAuditFormat() const {
            return _auditFormat;
        }

    private:
        bool _enabled;
        BSONObj _auditfilter;
        AuditFormat _auditFormat;

    } auditOptions;

    bool isEnabled() {
        return auditOptions.isEnabled();
    }

    AuditFormat getAuditFormat() {
        return auditOptions.getAuditFormat();
    }

} // namespace audit
} // namespace mongo
