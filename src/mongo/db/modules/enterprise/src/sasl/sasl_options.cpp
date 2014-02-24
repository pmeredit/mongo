/*
 *    Copyright (C) 2013 10gen Inc.
 */

#include "sasl_options.h"

#include "mongo/base/status.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {

    SASLGlobalParams saslGlobalParams;

    SASLGlobalParams::SASLGlobalParams() {
        authenticationMechanisms.push_back("MONGODB-CR");
        authenticationMechanisms.push_back("MONGODB-X509");
    }

    Status addSASLOptions(moe::OptionSection* options) {

        moe::OptionSection saslOptions("SASL Options");

        saslOptions.addOptionChaining("security.sasl.authenticationMechanisms", "",
                moe::StringVector, "List of supported authentication mechanisms.  "
                "Default is MONGODB-CR and MONGODB-X509.")
                                         .setSources(moe::SourceYAMLConfig);

        saslOptions.addOptionChaining("security.sasl.hostName", "", moe::String,
                "Fully qualified server domain name")
                                         .setSources(moe::SourceYAMLConfig);

        saslOptions.addOptionChaining("security.sasl.serviceName", "", moe::String,
                "Registered name of the service using SASL")
                                         .setSources(moe::SourceYAMLConfig);

        saslOptions.addOptionChaining("security.sasl.saslauthdSocketPath", "", moe::String,
                "Path to Unix domain socket file for saslauthd")
                                         .setSources(moe::SourceYAMLConfig);

        Status ret = options->addSection(saslOptions);
        if (!ret.isOK()) {
            log() << "Failed to add sasl option section: " << ret.toString();
            return ret;
        }

        return Status::OK();
    }

    Status storeSASLOptions(const moe::Environment& params) {

        bool haveAuthenticationMechanisms = false;
        bool haveHostName = false;
        bool haveServiceName = false;
        bool haveAuthdPath = false;

        // Check our setParameter options first so that these values can be properly overridden via
        // the command line even though the options have different names.
        if (params.count("setParameter")) {
            std::map<std::string, std::string> parameters =
                params["setParameter"].as<std::map<std::string, std::string> >();
            for (std::map<std::string, std::string>::iterator parametersIt = parameters.begin();
                 parametersIt != parameters.end(); parametersIt++) {
                if (parametersIt->first == "authenticationMechanisms") {
                    haveAuthenticationMechanisms = true;
                }
                else if (parametersIt->first == "saslHostName") {
                    haveHostName = true;
                }
                else if (parametersIt->first == "saslServiceName") {
                    haveServiceName = true;
                }
                else if (parametersIt->first == "saslauthdPath") {
                    haveAuthdPath = true;
                }
            }
        }

        if (params.count("security.sasl.authenticationMechanisms") &&
            !haveAuthenticationMechanisms) {
            saslGlobalParams.authenticationMechanisms =
                params["security.sasl.authenticationMechanisms"].as<std::vector<std::string> >();
        }
        if (params.count("security.sasl.hostName") && !haveHostName) {
            saslGlobalParams.hostName =
                params["security.sasl.hostName"].as<std::string>();
        }
        if (params.count("security.sasl.serviceName") && !haveServiceName) {
            saslGlobalParams.serviceName =
                params["security.sasl.serviceName"].as<std::string>();
        }
        if (params.count("security.sasl.saslauthdSocketPath") && !haveAuthdPath) {
            saslGlobalParams.authdPath =
                params["security.sasl.saslauthdSocketPath"].as<std::string>();
        }

        return Status::OK();
    }

    MONGO_MODULE_STARTUP_OPTIONS_REGISTER(SASLOptions)(InitializerContext* context) {
        return addSASLOptions(&moe::startupOptions);
    }

    MONGO_STARTUP_OPTIONS_STORE(SASLOptions)(InitializerContext* context) {
        return storeSASLOptions(moe::startupOptionsParsed);
    }

    // SASL Startup Parameters, making them settable via setParameter on the command line or in the
    // legacy INI config file.  None of these parameters are modifiable at runtime.
    ExportedServerParameter<std::vector<std::string> > SASLAuthenticationMechanismsSetting(
                                                        ServerParameterSet::getGlobal(),
                                                        "authenticationMechanisms",
                                                        &saslGlobalParams.authenticationMechanisms,
                                                        true, // Change at startup
                                                        false); // Change at runtime

    ExportedServerParameter<std::string> SASLHostNameSetting(ServerParameterSet::getGlobal(),
                                                             "saslHostName",
                                                              &saslGlobalParams.hostName,
                                                              true, // Change at startup
                                                              false); // Change at runtime

    ExportedServerParameter<std::string> SASLServiceNameSetting(ServerParameterSet::getGlobal(),
                                                                "saslServiceName",
                                                                 &saslGlobalParams.serviceName,
                                                                 true, // Change at startup
                                                                 false); // Change at runtime

    ExportedServerParameter<std::string> SASLAuthdPathSetting(ServerParameterSet::getGlobal(),
                                                              "saslauthdPath",
                                                              &saslGlobalParams.authdPath,
                                                              true, // Change at startup
                                                              false); // Change at runtime

} // namespace mongo
