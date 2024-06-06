/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
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
};

StatusWith<std::vector<File>> listDirectory(const BlockstoreHTTP& blockstore);

}  // namespace queryable

}  // namespace mongo
