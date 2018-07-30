/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <vector>

#include "mongo/base/status_with.h"

#include "blockstore_http.h"

namespace mongo {
namespace queryable {

struct File {
    std::string filename;
    std::int64_t fileSize;
    std::int32_t blockSize;
};

StatusWith<std::vector<File>> listDirectory(const BlockstoreHTTP& blockstore);

}  // namespace queryable

}  // namespace mongo
