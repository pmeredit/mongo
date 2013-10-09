/*
 *    Copyright (C) 2013 10gen Inc.
 */

#include "snmp_options.h"

#include "mongo/base/status.h"
#include "mongo/db/server_options.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"

namespace mongo {

    typedef moe::OptionDescription OD;
    typedef moe::PositionalOptionDescription POD;

    SnmpGlobalParams snmpGlobalParams;

    Status addSnmpOptions(moe::OptionSection* options) {

        typedef moe::OptionDescription OD;

        moe::OptionSection snmp_options("SNMP Module Options");

        Status ret = snmp_options.addOption(OD("snmp-subagent", "snmp-subagent", moe::Switch,
                    "run snmp subagent", true));
        if (!ret.isOK()) {
            return ret;
        }
        ret = snmp_options.addOption(OD("snmp-master", "snmp-master", moe::Switch,
                    "run snmp as master", true));
        if (!ret.isOK()) {
            return ret;
        }

        ret = options->addSection(snmp_options);
        if (!ret.isOK()) {
            return ret;
        }

        return Status::OK();
    }

    Status storeSnmpOptions(const moe::Environment& params,
                            const std::vector<std::string>& args) {

        if (params.count("snmp-subagent")) {
            snmpGlobalParams.enabled = true;
        }
        if (params.count("snmp-master")) {
            snmpGlobalParams.subagent = false;
            snmpGlobalParams.enabled = true;
        }

        return Status::OK();
    }

    MONGO_MODULE_STARTUP_OPTIONS_REGISTER(SnmpOptions)(InitializerContext* context) {
        return addSnmpOptions(&serverOptions);
    }

    MONGO_STARTUP_OPTIONS_STORE(SnmpOptions)(InitializerContext* context) {
        return storeSnmpOptions(serverParsedOptions, context->args());
    }

} // namespace mongo
