/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <iosfwd>

namespace mongo {

/**
 * Outputs the version of mongoqd as part of server startup.
 * Goes to `os` if nonnull, else to LOGV2.
 *
 * NOTE: Outputs the version of mongoqd to `os` (as part of the --version option),
 * which reports different data than if `os` is null!
 */
void logMongoqdVersionInfo(std::ostream* os);

}  // namespace mongo
