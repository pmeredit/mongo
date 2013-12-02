/*
 *    Copyright (C) 2013 10gen Inc.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/base/status.h"

namespace mongo {

namespace optionenvironment {
    class OptionSection;
    class Environment;
} // namespace optionenvironment

    namespace moe = optionenvironment;

    struct SASLGlobalParams {

        std::vector<std::string> authenticationMechanisms;
        std::string hostName;
        std::string serviceName;
        std::string authdPath;

        SASLGlobalParams();
    };

    extern SASLGlobalParams saslGlobalParams;

    Status addSASLOptions(moe::OptionSection* options);

    Status storeSASLOptions(const moe::Environment& params);

} // namespace mongo
