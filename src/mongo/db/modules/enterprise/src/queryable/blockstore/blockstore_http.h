/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/data_builder.h"
#include "mongo/base/data_range.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/oid.h"
#include "mongo/util/net/http_client.h"

namespace mongo {
namespace queryable {

const char* const kSecretKeyEnvVar = "SECRET_KEY";

class BlockstoreHTTP {
public:
    BlockstoreHTTP(StringData apiUrl,
                   mongo::OID snapshotId,
                   std::unique_ptr<HttpClient> client = std::unique_ptr<HttpClient>());

    StatusWith<std::size_t> read(StringData path,
                                 DataRange buf,
                                 std::size_t offset,
                                 std::size_t count) const;
    StatusWith<DataBuilder> write(StringData path,
                                  ConstDataRange buf,
                                  std::size_t offset,
                                  std::size_t count) const;
    StatusWith<DataBuilder> listDirectory() const;
    StatusWith<DataBuilder> openFile(StringData path) const;
    StatusWith<DataBuilder> renameFile(StringData from, StringData to) const;

private:
    std::string _apiUrl;
    mongo::OID _snapshotId;
    std::unique_ptr<HttpClient> _client;
};

}  // namespace queryable
}  // namespace mongo
