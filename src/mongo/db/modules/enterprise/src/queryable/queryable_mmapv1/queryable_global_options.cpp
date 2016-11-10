/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "queryable_global_options.h"

#include "mongo/bson/util/builder.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"

namespace mongo {
namespace queryable {

QueryableGlobalOptions queryableGlobalOptions;

Status QueryableGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection queryableOptions("Queryable Backup options");

    queryableOptions
        .addOptionChaining("queryableBackup.apiUri",
                           "queryableBackupApiUri",
                           moe::String,
                           "A connection string to a queryable API server")
        .hidden();

    queryableOptions
        .addOptionChaining("queryableBackup.snapshotId",
                           "queryableSnapshotId",
                           moe::String,
                           "A hex string of the OID for the snapshot to load")
        .hidden();

    queryableOptions
        .addOptionChaining("queryableBackup.memoryQuotaMB",
                           "queryableMemoryQuotaMB",
                           moe::Double,
                           "A number in MB indicating how much data queryable_mmapv1 "
                           "may keep in memory. If omitted, a default value will be "
                           "chosen.")
        .hidden();

    return options->addSection(queryableOptions);
}

Status QueryableGlobalOptions::store(const moe::Environment& params,
                                     const std::vector<std::string>& args) {
    if (params.count("queryableBackup.apiUri")) {
        auto uriStr = params["queryableBackup.apiUri"].as<std::string>();

        _apiUri = std::move(uriStr);
    }

    if (params.count("queryableBackup.snapshotId")) {
        auto snapshotStr = params["queryableBackup.snapshotId"].as<std::string>();
        if (snapshotStr.size() != 24) {
            return {ErrorCodes::BadValue,
                    str::stream() << "queryableBackup.snapshotId is not a valid OID: "
                                  << snapshotStr};
        }

        _snapshotId = OID(snapshotStr);
    }

    if (params.count("queryableBackup.memoryQuotaMB")) {
        auto memoryQuotaMB = params["queryableBackup.memoryQuotaMB"].as<double>();
        if (memoryQuotaMB <= 0) {
            return {ErrorCodes::BadValue,
                    str::stream() << "queryableBackup.memoryQuotaMB must be positive: "
                                  << memoryQuotaMB};
        }
        _memoryQuotaMB = memoryQuotaMB;
    }

    return Status::OK();
}

std::string QueryableGlobalOptions::getWiredTigerExtensionConfig(const std::string& dbpath) {
    StringBuilder wtConfig;
    wtConfig << "local={entry=queryableWtFsCreate,early_load=true,config={";
    wtConfig << "apiUri=\"" << *_apiUri << "\",";
    wtConfig << "snapshotId=\"" << *_snapshotId << "\",";
    wtConfig << "dbpath=\"" + dbpath + "\"}}";
    return wtConfig.str();
}


}  // namespace queryable

MONGO_MODULE_STARTUP_OPTIONS_REGISTER(QueryableOptions)(InitializerContext* context) {
    return queryable::queryableGlobalOptions.add(&moe::startupOptions);
}

MONGO_STARTUP_OPTIONS_VALIDATE(QueryableOptions)(InitializerContext* context) {
    return Status::OK();
}

MONGO_STARTUP_OPTIONS_STORE(QueryableOptions)(InitializerContext* context) {
    Status ret =
        queryable::queryableGlobalOptions.store(moe::startupOptionsParsed, context->args());
    if (!ret.isOK()) {
        severe() << ret.toString() << std::endl;
        severe() << "try '" << context->args()[0] << " --help' for more information" << std::endl;
        quickExit(EXIT_BADOPTIONS);
    }
    return Status::OK();
}

}  // namespace mongo
