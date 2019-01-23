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
        if (!swHosts.isOK()) {
            return swHosts.getStatus();
        }

        globalLDAPParams->serverHosts = std::move(swHosts.getValue());
    }

    if (params.count("security.ldap.transportSecurity")) {
        auto transportSecurity = params["security.ldap.transportSecurity"].as<std::string>();
        if (transportSecurity == "none") {
            globalLDAPParams->transportSecurity = LDAPTransportSecurityType::kNone;
        } else if (transportSecurity == "tls") {
            globalLDAPParams->transportSecurity = LDAPTransportSecurityType::kTLS;
        } else {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "Unrecognized transport security mechanism: "
                                        << transportSecurity);
        }
    }

    if (params.count("security.ldap.bind.method")) {
        auto swLDAPBindType =
            getLDAPBindType(params["security.ldap.bind.method"].as<std::string>());
        if (!swLDAPBindType.isOK()) {
            return swLDAPBindType.getStatus();
        }
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

    return Status::OK();
}


MONGO_INITIALIZER_GENERAL(LDAPOptions, ("SecureAllocator"), ("BeginStartupOptionStorage"))
(InitializerContext* context) {
    globalLDAPParams = new LDAPOptions();
    return Status::OK();
}

}  // namespace
}  // namespace mongo
