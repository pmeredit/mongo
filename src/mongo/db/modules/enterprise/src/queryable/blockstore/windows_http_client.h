/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "http_client.h"

#include "mongo/base/data_builder.h"

namespace mongo {
namespace queryable {

/**
 * Windows implementation of HttpClientInterface
 *
 * Uses WinInet, does not support Kerberos or other Windows authentication mechanisms.
 */
class WindowsHttpClient final : public HttpClientBase {
public:
    WindowsHttpClient(std::string apiUri, OID snapshotId);

    StatusWith<std::size_t> read(std::string path,
                                 DataRange buf,
                                 std::size_t offset,
                                 std::size_t count) const override;

    StatusWith<DataBuilder> listDirectory() const override;
};

}  // namespace queryable
}  // namespace mongo
