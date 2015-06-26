/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "ldap_options.h"

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

#include "authz_manager_external_state_ldap.h"
#include "ldap_connection_options.h"
#include "ldap_runner.h"

#include "mongo/util/log.h"

namespace mongo {
namespace {
namespace moe = mongo::optionenvironment;

Status addLDAPOptions(moe::OptionSection* options) {
    moe::OptionSection ldap_options("LDAP Module Options");

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.query.timeoutMS",
                                   "ldapQueryTimeoutMS",
                                   moe::Long,
                                   "TimeoutMS for LDAP queries").setDefault(moe::Value(10000));

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.bind.user",
                                   "ldapBindUser",
                                   moe::String,
                                   "LDAP entity to use for binding");

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.bind.saslMechanisms",
                                   "ldapBindSASLMechs",
                                   moe::String,
                                   "Comma separated list of SASL mechanisms to use while "
                                   "binding to the LDAP server")
        .setDefault(moe::Value(std::string("DIGEST-MD5")));

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.bind.method",
                                   "ldapBindMethod",
                                   moe::String,
                                   "Authentication scheme to use while connecting to LDAP. "
                                   "This may either be 'sasl' or 'simple'")
        .setDefault(moe::Value(std::string("sasl")));

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.bind.password",
                                   "ldapBindPassword",
                                   moe::String,
                                   "Password to use while binding to the LDAP server");

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.query.url",
                                   "ldapQueryUrl",
                                   moe::String,
                                   "Formatted string of format (ldap|ldaps)://host:port describing "
                                   "location of LDAP server");

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.query.template",
                                   "ldapQueryTemplate",
                                   moe::String,
                                   "Relative LDAP query URL which will be queried against the "
                                   "host to acquire LDAP groups. The token {USER} will be "
                                   "replaced with the mapped username");

    ldap_options.addOptionChaining("security.externalAuthorization.ldap.mapping.user",
                                   "ldapUserToDN",
                                   moe::String,
                                   "Tranformation from MongoDB users to LDAP user DNs")
        .setDefault(moe::Value(std::string("[{match: \"(.+)\", substitution: \"{0}\"}]")));

    Status ret = options->addSection(ldap_options);
    if (!ret.isOK()) {
        return ret;
    }

    return Status::OK();
}

Status storeLDAPOptions(const moe::Environment& params, const std::vector<std::string>& args) {
    if (params.count("security.externalAuthorization.ldap.query.url")) {
        globalLDAPParams.serverURI =
            params["security.externalAuthorization.ldap.query.url"].as<std::string>();
    }
    if (params.count("security.externalAuthorization.ldap.query.template")) {
        globalLDAPParams.userAcquisitionQueryTemplate =
            params["security.externalAuthorization.ldap.query.template"].as<std::string>();
    }
    if (params.count("security.externalAuthorization.ldap.query.timeoutMS")) {
        globalLDAPParams.connectionTimeout =
            Milliseconds(params["security.externalAuthorization.ldap.query.timeoutMS"].as<long>());
    }
    if (params.count("security.externalAuthorization.ldap.bind.user")) {
        globalLDAPParams.bindUser =
            params["security.externalAuthorization.ldap.bind.user"].as<std::string>();
    }
    if (params.count("security.externalAuthorization.ldap.bind.password")) {
        globalLDAPParams.bindPassword = SecureString(
            params["security.externalAuthorization.ldap.bind.password"].as<std::string>().c_str());
    }
    if (params.count("security.externalAuthorization.ldap.bind.method")) {
        auto swLDAPBindType = getLDAPBindType(
            params["security.externalAuthorization.ldap.bind.method"].as<std::string>());
        if (!swLDAPBindType.isOK()) {
            return swLDAPBindType.getStatus();
        }
        globalLDAPParams.bindMethod = swLDAPBindType.getValue();
    }
    if (params.count("security.externalAuthorization.ldap.bind.saslMechanisms")) {
        globalLDAPParams.bindSASLMechanisms =
            params["security.externalAuthorization.ldap.bind.saslMechanisms"].as<std::string>();
    }
    if (params.count("security.externalAuthorization.ldap.mapping.user")) {
        globalLDAPParams.userToDNMapping =
            params["security.externalAuthorization.ldap.mapping.user"].as<std::string>();
    }
    return Status::OK();
}

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(LDAPOptions)(InitializerContext* context) {
    return addLDAPOptions(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_STORE(LDAPOptions)(InitializerContext* context) {
    return storeLDAPOptions(moe::startupOptionsParsed, context->args());
}

}  // namespace

LDAPOptions globalLDAPParams;

}  // namespace mongo
