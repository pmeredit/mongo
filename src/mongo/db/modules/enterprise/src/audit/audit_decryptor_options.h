/*
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/status.h"

#include <boost/filesystem.hpp>

#include <string>
#include <vector>

#include "mongo/base/status.h"

namespace mongo {
namespace audit {

struct AuditDecryptorOptions {
    // Path to resource to decrypt
    boost::filesystem::path inputPath;

    // Path to where decrypted file will be written
    boost::filesystem::path outputPath;

    // If true, do not ask for confirmation before decryption
    bool confirmDecryption = true;
};

extern AuditDecryptorOptions globalAuditDecryptorOptions;

}  // namespace audit
}  // namespace mongo
