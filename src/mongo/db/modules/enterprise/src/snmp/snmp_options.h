/*
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <vector>

#include "mongo/base/status.h"

namespace mongo {

    namespace optionenvironment {
        class Environment;
        class OptionSection;
    } // namespace optionenvironment

    namespace moe = mongo::optionenvironment;

    struct SnmpGlobalParams {

        SnmpGlobalParams() : enabled(false), subagent(true) {}

        bool enabled;
        bool subagent;
    };

    extern SnmpGlobalParams snmpGlobalParams;

    Status addSnmpOptions(moe::OptionSection* options);

    Status storeSnmpOptions(const moe::Environment& params,
                            const std::vector<std::string>& args);
}
