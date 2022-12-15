/**
 * Copyright (C) 2022 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "magic_restore.h"

#include <boost/filesystem/operations.hpp>
#include <memory>

#include "mongo/bson/json.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_impl.h"
#include "mongo/db/catalog/database_holder_impl.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/modules/enterprise/src/magic_restore/magic_restore_options_gen.h"
#include "mongo/db/mongod_options.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/logv2/log.h"
#include "mongo/util/exit.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

namespace mongo {

namespace moe = mongo::optionenvironment;

ExitCode magicRestoreMain(ServiceContext* svcCtx) {
    BSONObj restoreConfigObj;
    auto opCtx = cc().makeOperationContext();

    moe::Environment& params = moe::startupOptionsParsed;
    if (params.count("restoreConfiguration")) {
        const std::string restoreConfigFilename = params["restoreConfiguration"].as<std::string>();

        std::string configContents;
        Status status =
            moe::readRawFile(restoreConfigFilename, &configContents, moe::ConfigExpand{});
        restoreConfigObj = fromjson(configContents);
    }

    if (params.count("additionalOplogEntriesFile")) {
    }

    if (params.count("additionalOplogEntriesViaStdIn")) {
    }

    // Take unstable checkpoints from here on out. Nothing done as part of a restore is replication
    // rollback safe.
    svcCtx->getStorageEngine()->setInitialDataTimestamp(
        Timestamp::kAllowUnstableCheckpointsSentinel);

    auto replProcess = repl::ReplicationProcess::get(svcCtx);
    BSONElement pointInTimeTimestamp = restoreConfigObj["pointInTimeTimestamp"];
    if (pointInTimeTimestamp.ok()) {
        fassertNoTrace(7190501, pointInTimeTimestamp.type() == bsonTimestamp);
        replProcess->getConsistencyMarkers()->setOplogTruncateAfterPoint(
            opCtx.get(), pointInTimeTimestamp.timestamp());
    }

    auto* storageInterface = repl::StorageInterface::get(svcCtx);
    fassert(7197101,
            storageInterface->truncateCollection(opCtx.get(),
                                                 NamespaceString::kSystemReplSetNamespace));
    fassert(7197102,
            storageInterface->putSingleton(opCtx.get(),
                                           NamespaceString::kSystemReplSetNamespace,
                                           {restoreConfigObj["replicaSetConfig"].Obj()}));
    exitCleanly(ExitCode::clean);
    return ExitCode::clean;
}

MONGO_STARTUP_OPTIONS_POST(MagicRestore)(InitializerContext*) {
    setMagicRestoreMain(magicRestoreMain);
}
}  // namespace mongo
