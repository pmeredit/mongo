/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "queryable_mmap_v1_fs_helpers.h"

#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace queryable {

std::string removeDirectory(std::string filepath) {
    // Remove the last directory separator. Don't assume the local OS is the same as what created
    // the snapshot. Treat both back and forward slashes as directory separates. Assumes database
    // names cannot contain slashes, which is true for at least >= 3.2
    auto lastIdx = filepath.find_last_of("/\\");
    if (lastIdx == std::string::npos) {
        return filepath;
    }

    return filepath.substr(lastIdx + 1);
}

std::regex getMMAPV1DatafileRegex(std::string dbname) {
    // Also agnostic to filepath separators. Assumes neither back nor forward slashes are allowed in
    // database names.
    std::string withoutDirectoryPerDb(dbname);
    std::string withDirectoryPerDb = str::stream() << dbname << "[/\\\\]" << dbname;
    std::string prefix = str::stream() << "(?:" << withDirectoryPerDb << "|"
                                       << withoutDirectoryPerDb << ")";

    return std::regex(std::string(str::stream() << "^" << prefix << "\\.(\\d+)$"));
}

}  // namespace queryable
}  // namespace mongo
