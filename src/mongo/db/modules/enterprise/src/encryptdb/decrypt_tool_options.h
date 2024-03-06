/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#pragma once

#include "mongo/base/status.h"

#include <boost/filesystem.hpp>

#include <string>
#include <vector>

#include "mongo/base/status.h"

#include "kmip_options.h"
#include "symmetric_crypto.h"

namespace mongo {

struct DecryptToolOptions {
    // Path to resource to decrypt
    boost::filesystem::path inputPath;

    // Path to where decrypted file will be written
    boost::filesystem::path outputPath;

    // The ciphermode that the input was encrypted with
    crypto::aesMode mode;

    // Parameters to use to speak with KMIP server
    KMIPParams kmipParams;

    // Path to a keyfile on disk
    boost::filesystem::path keyFile;

    // If true, do not ask for confirmation before decryption
    bool confirmDecryption = true;
};

extern DecryptToolOptions globalDecryptToolOptions;

}  // mongo
