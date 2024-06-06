/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "mongoqd_options.h"

#include <iostream>
#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/util/builder.h"
#include "mongo/config.h"
#include "mongo/db/server_options_base.h"
#include "mongo/db/server_options_server_helpers.h"
#include "mongo/logv2/log.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/str.h"
#include "version_mongoqd.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding


namespace mongo {

MongoqdGlobalParams mongoqdGlobalParams;

void printMongoqdHelp(const moe::OptionSection& options) {
    std::cout << options.helpString() << std::endl;
};

bool handlePreValidationMongoqdOptions(const moe::Environment& params,
                                       const std::vector<std::string>& args) {
    if (params.count("help") && params["help"].as<bool>() == true) {
        printMongoqdHelp(moe::startupOptions);
        return false;
    }
    if (params.count("version") && params["version"].as<bool>() == true) {
        logMongoqdVersionInfo(&std::cout);
        return false;
    }
    if (params.count("test") && params["test"].as<bool>() == true) {
        logv2::LogManager::global().getGlobalSettings().setMinimumLoggedSeverity(
            mongo::logv2::LogComponent::kDefault, ::mongo::logv2::LogSeverity::Debug(5));
        return false;
    }

    return true;
}

Status validateMongoqdOptions(const moe::Environment& params) {
    Status ret = validateServerOptions(params);
    if (!ret.isOK()) {
        return ret;
    }

    return Status::OK();
}

Status canonicalizeMongoqdOptions(moe::Environment* params) {
    Status ret = canonicalizeServerOptions(params);
    if (!ret.isOK()) {
        return ret;
    }

    // "security.javascriptEnabled" comes from the config file, so override it if "noscripting"
    // is set since that comes from the command line.
    if (params->count("noscripting")) {
        auto status = params->set("security.javascriptEnabled",
                                  moe::Value(!(*params)["noscripting"].as<bool>()));
        if (!status.isOK()) {
            return status;
        }

        status = params->remove("noscripting");
        if (!status.isOK()) {
            return status;
        }
    }

    return Status::OK();
}

Status storeMongoqdOptions(const moe::Environment& params) {
    Status ret = storeServerOptions(params);
    if (!ret.isOK()) {
        return ret;
    }

    if (params.count("security.javascriptEnabled")) {
        mongoqdGlobalParams.scriptingEnabled = params["security.javascriptEnabled"].as<bool>();
    }

    if (!params.count("sharding.configDB")) {
        return Status(ErrorCodes::BadValue, "error: no args for --configdb");
    }

    std::string configdbString = params["sharding.configDB"].as<std::string>();

    auto configdbConnectionString = ConnectionString::parse(configdbString);
    if (!configdbConnectionString.isOK()) {
        return configdbConnectionString.getStatus();
    }

    if (configdbConnectionString.getValue().type() !=
        ConnectionString::ConnectionType::kReplicaSet) {
        return Status(ErrorCodes::BadValue,
                      str::stream() << "configdb supports only replica set connection string");
    }

    std::vector<HostAndPort> seedServers;
    bool resolvedSomeSeedSever = false;
    for (const auto& host : configdbConnectionString.getValue().getServers()) {
        seedServers.push_back(host);
        if (!seedServers.back().hasPort()) {
            seedServers.back() = HostAndPort{host.host(), ServerGlobalParams::ConfigServerPort};
        }
        if (!hostbyname(seedServers.back().host().c_str()).empty()) {
            resolvedSomeSeedSever = true;
        }
    }
    if (!resolvedSomeSeedSever) {
        if (!hostbyname(configdbConnectionString.getValue().getSetName().c_str()).empty()) {
            LOGV2_WARNING(6184401,
                          "The replica set name "
                          "\"{str_escape_configdbConnectionString_getValue_getSetName}\" resolves "
                          "as a host name, but none of the servers in the seed list do. "
                          "Did you reverse the replica set name and the seed list in "
                          "{str_escape_configdbConnectionString_getValue}?",
                          "str_escape_configdbConnectionString_getValue_getSetName"_attr =
                              str::escape(configdbConnectionString.getValue().getSetName()),
                          "str_escape_configdbConnectionString_getValue"_attr =
                              str::escape(configdbConnectionString.getValue().toString()));
        }
    }

    mongoqdGlobalParams.configdbs =
        ConnectionString{configdbConnectionString.getValue().type(),
                         seedServers,
                         configdbConnectionString.getValue().getSetName()};

    if (mongoqdGlobalParams.configdbs.getServers().size() < 3) {
        LOGV2_WARNING(6184400,
                      "Running a sharded cluster with fewer than 3 config servers should only be "
                      "done for testing purposes and is not recommended for production.");
    }

    return Status::OK();
}

}  // namespace mongo
