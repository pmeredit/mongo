/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "queryable_global_options.h"

#include "mongo/bson/util/builder.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace queryable {

QueryableGlobalOptions queryableGlobalOptions;

Status QueryableGlobalOptions::store() {
    const auto& params = optionenvironment::startupOptionsParsed;

    if (params.count("queryableBackup.apiUri")) {
        _apiUri = params["queryableBackup.apiUri"].as<std::string>();
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

    return Status::OK();
}

namespace {
MONGO_STARTUP_OPTIONS_STORE(QueryableOptions)(InitializerContext* context) {
    return queryable::queryableGlobalOptions.store();
}
}  // namespace

std::string QueryableGlobalOptions::getWiredTigerExtensionConfig(const std::string& dbpath) {
    StringBuilder wtConfig;
    wtConfig << "local={entry=queryableWtFsCreate,early_load=true,config={";
    wtConfig << "apiUri=\"" << *_apiUri << "\",";
    wtConfig << "snapshotId=\"" << *_snapshotId << "\",";
    wtConfig << "dbpath=\"" + dbpath + "\"}}";
    return wtConfig.str();
}

}  // namespace queryable
}  // namespace mongo
