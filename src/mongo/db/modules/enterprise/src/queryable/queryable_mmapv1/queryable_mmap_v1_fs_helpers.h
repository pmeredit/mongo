/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <regex>
#include <string>

namespace mongo {
namespace queryable {

/**
 * "a/b/c/file.ns" -> "file.ns"
 */
std::string removeDirectory(std::string filepath);

/**
 * Return a regex that matches mmapv1 datafiles (*.<number>) and groups the number. The regex should
 * match filepaths regardless of whether `directoryperdb` is enabled. E.g:
 * "dbname/dbname.10" -> (true, "10"), "notdbname/dbname.10" -> (false)
 */
std::regex getMMAPV1DatafileRegex(std::string dbname);

}  // namespace queryable
}  // namespace mongo
