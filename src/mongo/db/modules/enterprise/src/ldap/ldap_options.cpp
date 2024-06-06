/**
 *  Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "ldap_options.h"

#include "mongo/base/status.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/startup_option_init.h"

#include "ldap_connection_options.h"
#include "ldap_host.h"

namespace mongo {

LDAPOptions* globalLDAPParams = nullptr;

namespace {

MONGO_STARTUP_OPTIONS_STORE(LDAPOptions)(InitializerContext* context) {
    const auto& params = optionenvironment::startupOptionsParsed;

    if (params.count("security.ldap.transportSecurity")) {
        auto transportSecurity = params["security.ldap.transportSecurity"].as<std::string>();
        if (transportSecurity == "none") {
            globalLDAPParams->transportSecurity = LDAPTransportSecurityType::kNone;
        } else if (transportSecurity == "tls") {
            globalLDAPParams->transportSecurity = LDAPTransportSecurityType::kTLS;
        } else {
            uasserted(ErrorCodes::FailedToParse,
                      str::stream()
                          << "Unrecognized transport security mechanism: " << transportSecurity);
        }
    }

    if (params.count("security.ldap.servers")) {
        bool isSSL = (globalLDAPParams->transportSecurity == LDAPTransportSecurityType::kTLS);
        StatusWith<std::vector<LDAPHost>> swHosts = LDAPConnectionOptions::parseHostURIs(
            params["security.ldap.servers"].as<std::string>(), isSSL);
        uassertStatusOK(swHosts);
        globalLDAPParams->serverHosts = std::move(swHosts.getValue());
    }

    if (params.count("security.ldap.bind.method")) {
        auto swLDAPBindType =
            getLDAPBindType(params["security.ldap.bind.method"].as<std::string>());
        uassertStatusOK(swLDAPBindType);
        globalLDAPParams->bindMethod = swLDAPBindType.getValue();
    }

    if (params.count("security.ldap.timeoutMS")) {
        globalLDAPParams->connectionTimeout =
            Milliseconds(params["security.ldap.timeoutMS"].as<int>());
    }

    if (params.count("security.ldap.retryCount")) {
        globalLDAPParams->retryCount = params["security.ldap.retryCount"].as<int>();
    }

    if (params.count("security.ldap.bind.queryPassword")) {
        globalLDAPParams->bindPassword =
            SecureString(params["security.ldap.bind.queryPassword"].as<std::string>().c_str());
    }

    if (params.count("security.ldap.serverCAFile")) {
        globalLDAPParams->serverCAFile = params["security.ldap.serverCAFile"].as<std::string>();
    }
}


MONGO_INITIALIZER_GENERAL(LDAPOptions, ("SecureAllocator"), ("BeginStartupOptionStorage"))
(InitializerContext* context) {
    globalLDAPParams = new LDAPOptions();
}

}  // namespace
}  // namespace mongo
