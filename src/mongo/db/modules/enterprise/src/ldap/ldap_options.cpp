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

    ldap_options.addOptionChaining("security.ldap.server",
                                   "ldapServer",
                                   moe::String,
                                   "Location of LDAP server on format (ldap|ldaps)://host:port");

    ldap_options.addOptionChaining("security.ldap.bind.method",
                                   "ldapBindMethod",
                                   moe::String,
                                   "Authentication scheme to use while connecting to LDAP. "
                                   "This may either be 'sasl' or 'simple'")
        .setDefault(moe::Value(std::string("sasl")));

    ldap_options.addOptionChaining("security.ldap.bind.saslMechanisms",
                                   "ldapBindSASLMechs",
                                   moe::String,
                                   "Comma separated list of SASL mechanisms to use while "
                                   "binding to the LDAP server")
        .setDefault(moe::Value(std::string("DIGEST-MD5")));

    ldap_options.addOptionChaining("security.ldap.timeoutMS",
                                   "ldapTimeoutMS",
                                   moe::Long,
                                   "Timeout for LDAP queries (ms)").setDefault(moe::Value(10000));

    ldap_options.addOptionChaining("security.ldap.bind.queryUser",
                                   "ldapQueryUser",
                                   moe::String,
                                   "LDAP entity to bind with to perform queries");

    ldap_options.addOptionChaining(
        "security.ldap.bind.queryPassword",
        "ldapQueryPassword",
        moe::String,
        "Password to use while binding to the LDAP server to perform queries");

    ldap_options.addOptionChaining("security.ldap.authz.queryTemplate",
                                   "ldapAuthzQueryTemplate",
                                   moe::String,
                                   "Relative LDAP query URL which will be queried against the "
                                   "host to acquire LDAP groups. The token {USER} will be "
                                   "replaced with the mapped username");

    ldap_options.addOptionChaining("security.ldap.userToDNMapping",
                                   "ldapUserToDNMapping",
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
    if (params.count("security.ldap.server")) {
        globalLDAPParams->serverURI = params["security.ldap.server"].as<std::string>();
    }
    if (params.count("security.ldap.bind.method")) {
        auto swLDAPBindType =
            getLDAPBindType(params["security.ldap.bind.method"].as<std::string>());
        if (!swLDAPBindType.isOK()) {
            return swLDAPBindType.getStatus();
        }
        globalLDAPParams->bindMethod = swLDAPBindType.getValue();
    }
    if (params.count("security.ldap.bind.saslMechanisms")) {
        globalLDAPParams->bindSASLMechanisms =
            params["security.ldap.bind.saslMechanisms"].as<std::string>();
    }
    if (params.count("security.ldap.timeoutMS")) {
        globalLDAPParams->connectionTimeout =
            Milliseconds(params["security.ldap.timeoutMS"].as<long>());
    }
    if (params.count("security.ldap.bind.queryUser")) {
        globalLDAPParams->bindUser = params["security.ldap.bind.queryUser"].as<std::string>();
    }
    if (params.count("security.ldap.bind.queryPassword")) {
        globalLDAPParams->bindPassword =
            SecureString(params["security.ldap.bind.queryPassword"].as<std::string>().c_str());
    }
    if (params.count("security.ldap.authz.queryTemplate")) {
        globalLDAPParams->userAcquisitionQueryTemplate =
            params["security.ldap.authz.queryTemplate"].as<std::string>();
    }
    if (params.count("security.ldap.userToDNMapping")) {
        globalLDAPParams->userToDNMapping =
            params["security.ldap.userToDNMapping"].as<std::string>();
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

MONGO_INITIALIZER_GENERAL(LDAPOptions,
                          ("SecureAllocator"),
                          ("LDAPOptions_Store"))(InitializerContext* context) {
    globalLDAPParams = new LDAPOptions();
    return Status::OK();
}

LDAPOptions* globalLDAPParams;

}  // namespace mongo
