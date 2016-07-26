/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <vector>

#include "mongo/base/status_with.h"

#include "http_client.h"

namespace mongo {
namespace queryable {

struct File {
    std::string filename;
    std::int64_t fileSize;
    std::int32_t blockSize;
};

StatusWith<std::vector<File>> listDirectory(HttpClientInterface* httpClient);

}  // namespace queryable

}  // namespace mongo
