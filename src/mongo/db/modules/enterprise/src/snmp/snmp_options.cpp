/*
 *    Copyright (C) 2019 10gen Inc.
 */

#include "snmp_options.h"

#include "mongo/base/status.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

SnmpGlobalParams snmpGlobalParams;

namespace {
MONGO_STARTUP_OPTIONS_STORE(SnmpOptions)(InitializerContext*) {
    const auto& params = optionenvironment::startupOptionsParsed;

    if (params.count("snmp.subagent") && params["snmp.subagent"].as<bool>()) {
        snmpGlobalParams.enabled = true;
    }
    if (params.count("snmp.master") && params["snmp.master"].as<bool>()) {
        snmpGlobalParams.subagent = false;
        snmpGlobalParams.enabled = true;
    }

    return Status::OK();
}
}  // namespace

}  // namespace mongo
