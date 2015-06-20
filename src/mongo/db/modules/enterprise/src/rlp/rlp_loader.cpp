/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "rlp_loader.h"

#include <boost/filesystem.hpp>
#include <string>

#include "mongo/platform/shared_library.h"

#include "rlp_context.h"

namespace mongo {
namespace fts {
namespace {

#ifdef __APPLE__
const char kDynamicLibrarySuffix[] = ".dylib";
#elif defined(_WIN32)
const char kDynamicLibrarySuffix[] = ".dll";
#else
const char kDynamicLibrarySuffix[] = ".so";
#endif

// RLP Libraries
//
const char kCore[] = "libbtrlpcore";
const char kCoreC[] = "libbtrlpc";
const char kUtils[] = "libbtutils";

boost::filesystem::path getLibraryPath(std::string btRoot, const char* library) {
    boost::filesystem::path full_path(btRoot);

#ifdef __APPLE__
    full_path /= "/rlp/lib/universal-darwin9-gcc40/";
#elif defined(__linux__)
    full_path /= "/rlp/lib/amd64-glibc25-gcc41/";
#else
    full_path /= "?";
#endif
    full_path /= library;

    full_path += kDynamicLibrarySuffix;

    return full_path;
}
}

StatusWith<std::unique_ptr<RlpLoader>> RlpLoader::create(std::string btRoot, bool verbose) {
    // On Linux, we must load the C API, and its two dependent libraries otherwise
    // RLP does not set $ORIGIN
    // Note: We only need to keep a reference alive to the C library since it
    // depends on the others
    //
    auto swl = SharedLibrary::create(getLibraryPath(btRoot, kUtils));
    if (!swl.getStatus().isOK()) {
        return swl.getStatus();
    }

    auto libraryUtils = std::move(swl.getValue());

    swl = SharedLibrary::create(getLibraryPath(btRoot, kCore));
    if (!swl.getStatus().isOK()) {
        return swl.getStatus();
    }

    auto libraryCore = std::move(swl.getValue());

    swl = SharedLibrary::create(getLibraryPath(btRoot, kCoreC));
    if (!swl.getStatus().isOK()) {
        return swl.getStatus();
    }

    auto libraryCoreC = std::move(swl.getValue());

    StatusWith<std::unique_ptr<RlpEnvironment>> swe =
        RlpEnvironment::create(btRoot, verbose, libraryCoreC.get());

    if (!swe.isOK()) {
        return swe.getStatus();
    }

    std::unique_ptr<RlpLoader> rlpLoader(
        new RlpLoader(std::move(libraryCoreC), std::move(swe.getValue())));

    return StatusWith<std::unique_ptr<RlpLoader>>(std::move(rlpLoader));
}

}  // namespace fts
}  // namespace mongo
