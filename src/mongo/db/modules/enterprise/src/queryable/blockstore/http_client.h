/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/base/data_builder.h"
#include "mongo/base/data_range.h"
#include "mongo/base/disallow_copying.h"
#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/oid.h"

namespace mongo {
namespace queryable {

class HttpClientInterface {
    MONGO_DISALLOW_COPYING(HttpClientInterface);

public:
    virtual ~HttpClientInterface() = default;

    virtual StatusWith<std::size_t> read(std::string path,
                                         DataRange buf,
                                         std::size_t offset,
                                         std::size_t count) const = 0;

    virtual StatusWith<DataBuilder> listDirectory() const = 0;

protected:
    HttpClientInterface() = default;
};

}  // namespace queryable

}  // namespace mongo
