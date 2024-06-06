/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional.hpp>
#include <filesystem>
#include <fmt/format.h>
#include <string>

#include "streams/exec/message.h"

// This file has some internal helpers that are not part of the public interface
namespace streams {

// At ~64MB per file, this should give us > 600GB of state per checkpoint
constexpr int kMaxStateFileIdx = 10000;

// Given an index like 64, returns "state00064.bin"
std::string getStateFileNameFromIdx(int fileIdx);

// Forms a path like: rootDir/state0064.bin. If the optional suffix
// argument is provided, appends that the end. (Currently the suffix arg is used to specify the
// snappy suffix - .sz)
std::filesystem::path getStateFilePath(const std::filesystem::path& rootDir,
                                       int fileIdx,
                                       const std::string& suffix = "");

// Forms a path like: rootDir/MANIFEST
std::filesystem::path getManifestFilePath(const std::filesystem::path& rootDir);

// A file named fname will have fname.shadow as its shadow file. When writing either the state or
// the manifest files, we first write to the shadow file and then atomically rename to the real name
// This helps in ensuring that an external tool (like the Go agent) will  not read a file while it
// is still being written
std::string getShadowFilePath(const std::filesystem::path& fname);

// Read entire file into a contiguous buffer
// Will throw a std::runtime_error exception on failure
std::string readFile(const std::string& path);

// Write entire file to disk. If the optional checksum is provided, it is
// written as a 4 byte preamble. The file is expected to not already exist and will be overwritten
// if present
// Will throw a std::runtime_error exception on failure
void writeFile(const std::string& fName,
               const char* data,
               size_t datalen,
               boost::optional<uint32_t> checksum);

// Computes the checksum of the input data
uint32_t getChecksum32(const char* data, size_t len);

}  // namespace streams
