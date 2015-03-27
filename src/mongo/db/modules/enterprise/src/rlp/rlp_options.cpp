/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "rlp_options.h"

#include "mongo/base/status.h"
#include "mongo/db/server_options.h"
#include "mongo/logger/log_severity.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace fts {

    namespace moe = optionenvironment;

    RLPGlobalParams rlpGlobalParams;

namespace {
    const char kBtRootOptionLong[] = "basisTech.RootDirectory";
    const char kBtRootOptionShort[] = "basisTechRootDirectory";

    Status addRLPOptions(moe::OptionSection* options) {
        moe::OptionSection rlpOptions("Rosette Linguistics Platform Options");

        rlpOptions.addOptionChaining(
            kBtRootOptionLong,
            kBtRootOptionShort,
            moe::String,
            "Root directory of a Basis Technology installation, i.e. BT_ROOT");

        Status ret = options->addSection(rlpOptions);

        if (!ret.isOK()) {
            error() << "Failed to add btRoot option section: " << ret.toString();
            return ret;
        }

        return Status::OK();
    }

    Status storeRLPOptions(const moe::Environment& params) {
        if (params.count(kBtRootOptionLong)) {
            rlpGlobalParams.btRoot = params[kBtRootOptionLong].as<std::string>();
        }

        return Status::OK();
    }
}

    MONGO_MODULE_STARTUP_OPTIONS_REGISTER(RLPOptions)(InitializerContext* context) {
        return addRLPOptions(&moe::startupOptions);
    }

    MONGO_STARTUP_OPTIONS_STORE(RLPOptions)(InitializerContext* context) {
        return storeRLPOptions(moe::startupOptionsParsed);
    }

}  // namespace fts
}  // namespace mongo
