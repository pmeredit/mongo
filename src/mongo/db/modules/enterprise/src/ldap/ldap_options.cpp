/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "ldap_options.h"

#include "mongo/base/status.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/startup_option_init.h"

#include "ldap_connection_options.h"

namespace mongo {

LDAPOptions* globalLDAPParams = nullptr;

namespace {

MONGO_STARTUP_OPTIONS_STORE(LDAPOptions)(InitializerContext* context) {
    const auto& params = optionenvironment::startupOptionsParsed;

    if (params.count("security.ldap.servers")) {
        StatusWith<std::vector<std::string>> swHosts =
            LDAPConnectionOptions::parseHostURIs(params["security.ldap.servers"].as<std::string>());
        uassertStatusOK(swHosts);
        globalLDAPParams->serverHosts = std::move(swHosts.getValue());
    }

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

    if (params.count("security.ldap.bind.method")) {
        auto swLDAPBindType =
            getLDAPBindType(params["security.ldap.bind.method"].as<std::string>());
        uassertStatusOK(swLDAPBindType);
        globalLDAPParams->bindMethod = swLDAPBindType.getValue();
    }

    if (params.count("security.ldap.timeoutMS")) {
        globalLDAPParams->connectionTimeout =
            Milliseconds(params["security.ldap.timeoutMS"].as<long>());
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
