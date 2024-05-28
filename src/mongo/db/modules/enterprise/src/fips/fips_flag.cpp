/**
 *    Copyright (C) 2021 MongoDB Inc.
 */


#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/logv2/log.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kNetwork


namespace mongo {
namespace {

/**
 * Helper for activating on either net.tls.FIPSMode or tls.FIPSMode.
 * The former is used in server mode, the latter in client mode.
 */
bool isEnabled(const std::string& key) {
    const auto& params = mongo::optionenvironment::startupOptionsParsed;
    return params.count(key) && params[key].as<bool>();
}

/**
 * Scans current saslGlobalOptions for usage of SCRAM-SHA-1.
 */
bool hasSCRAMSHA1() {
    return std::any_of(saslGlobalParams.authenticationMechanisms.cbegin(),
                       saslGlobalParams.authenticationMechanisms.cend(),
                       [](const auto& v) { return v == "SCRAM-SHA-1"_sd; });
}

/**
 * When in FIPS mode, we want to disable SCRAM-SHA-1 as an authentication mechanism
 * unless it has been explicit enabled via setParameter.
 */
MONGO_STARTUP_OPTIONS_POST(FipsOptionInit)(InitializerContext*) {
    const auto& params = mongo::optionenvironment::startupOptionsParsed;

    if (!isEnabled("net.tls.FIPSMode") && !isEnabled("tls.FIPSMode")) {
        // FIPS mode not enabled.
        return;
    }

    if (params.count("setParameter")) {
        auto sp = params["setParameter"].as<std::map<std::string, std::string>>();
        if (sp.find("authenticationMechanisms") != sp.end()) {
            // Explicit override via configuration, accept it.
            return;
        }
    }

    if (!hasSCRAMSHA1()) {
        // SCRAM-SHA-1 isn't enabled to begin with.
        // As of this writing, this will never pass,
        // but this protects future-us from wasting too much time.
        return;
    }

    // FIPS mode is enabled, and the config does not explicitly ask for SCRAM-SHA-1.
    // Prefer std::erase() in C++20, but for now do a simple copy and replace.
    LOGV2_DEBUG(5952800, 1, "Implicitly disabling SCRAM-SHA-1 due to FIPS mode");
    auto& authMechs = saslGlobalParams.authenticationMechanisms;
    typename std::remove_reference_t<decltype(authMechs)> mechs;
    std::copy_if(authMechs.cbegin(),
                 authMechs.cend(),
                 std::back_inserter(mechs),
                 [](const auto& v) { return v != "SCRAM-SHA-1"_sd; });
    authMechs = std::move(mechs);

    invariant(!hasSCRAMSHA1());
}

}  // namespace
}  // namespace mongo
