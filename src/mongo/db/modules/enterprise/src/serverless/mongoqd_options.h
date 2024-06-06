/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/status.h"
#include "mongo/client/connection_string.h"
#include "mongo/db/server_options.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_section.h"

namespace mongo {

namespace optionenvironment {
class OptionSection;
class Environment;
}  // namespace optionenvironment

namespace moe = mongo::optionenvironment;

struct MongoqdGlobalParams {
    bool scriptingEnabled = true;  // Use "security.javascriptEnabled" to set this variable. Or use
                                   // --noscripting which will set it to false.

    // The config server connection string
    ConnectionString configdbs;
};

extern MongoqdGlobalParams mongoqdGlobalParams;

void printMongoqdHelp(const moe::OptionSection& options);

/**
 * Handle options that should come before validation, such as "help".
 *
 * Returns false if an option was found that implies we should prematurely exit with success.
 */
bool handlePreValidationMongoqdOptions(const moe::Environment& params,
                                       const std::vector<std::string>& args);

/**
 * Handle custom validation of mongoqd options that can not currently be done by using
 * Constraints in the Environment.  See the "validate" function in the Environment class for
 * more details.
 */
Status validateMongoqdOptions(const moe::Environment& params);

/**
 * Canonicalize mongoqd options for the given environment.
 *
 * For example, the options "dur", "nodur", "journal", "nojournal", and
 * "storage.journaling.enabled" should all be merged into "storage.journaling.enabled".
 */
Status canonicalizeMongoqdOptions(moe::Environment* params);

Status storeMongoqdOptions(const moe::Environment& params);
}  // namespace mongo
