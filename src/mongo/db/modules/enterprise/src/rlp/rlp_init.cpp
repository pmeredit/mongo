/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/db/server_parameters.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "rlp_language.h"
#include "rlp_loader.h"
#include "rlp_options.h"

namespace mongo {
namespace fts {

    // Global System wide RLP Loader
    // This keeps the RlpLoader alive for the lifetime of the process
    //
    RlpLoader* rlpLoader;

    /**
     * Verbose Basis Tech RLP Logging
     *
     * Useful for troubleshooting RLP license issues.
     * Note: has a performance impact
     * Enables Error, Warning, and Info logging levels in RLP
     */
    MONGO_EXPORT_STARTUP_SERVER_PARAMETER(rlpVerbose, bool, false);

    MONGO_INITIALIZER_GENERAL(InitRLP,
                              ("EndStartupOptionHandling"),
                              ("default"))(InitializerContext* context) {
        if (rlpGlobalParams.btRoot.empty()) {
            LOG(0) << "Skipping RLP Initialization, BT Root not set.";
            return Status::OK();
        }

        StatusWith<std::unique_ptr<RlpLoader>> sw =
            RlpLoader::create(rlpGlobalParams.btRoot, rlpVerbose);

        if (sw.getStatus().isOK()) {
            rlpLoader = sw.getValue().release();
        }

        return sw.getStatus();
    }

    // Register languages so they can be resolved at runtime
    //
    MONGO_INITIALIZER_WITH_PREREQUISITES(RlpLangInit, ("FTSAllLanguagesRegistered",
        "FTSRegisterLanguageAliases",
        "InitRLP"))
    (::mongo::InitializerContext* context) {
        // If user did not specify RLP, it is ok
        if (rlpLoader) {
            registerRlpLanguages(rlpLoader->getEnvironment());
        }

        return Status::OK();
    }

}  // namespace fts
}  // namespace mongo
