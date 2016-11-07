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

/**
 * Http Client interface to blockstore HTTP server.
 */
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

/**
 * Base class for Http Client implementations.
 */
class HttpClientBase : public HttpClientInterface {
protected:
    HttpClientBase(std::string apiUri, OID snapshotId);

    /**
     * Get the url to get a snapshot block(s).
     */
    std::string getSnapshotUrl(StringData path, std::size_t offset, std::size_t count) const;

    /**
     * Get the url to list a directory in the blockstore.
     */
    std::string getListDirectoryUrl() const;

    /**
     * Get the secret header formatted as "Secret: value".
     */
    std::string getSecretHeader() const;

private:
    std::string _apiUri;
    OID _snapshotId;
    std::string _authSecret;
};

/**
 * Create an implementation of HttpClientInterface for the specified appUri and snapshotId.
 */
std::unique_ptr<HttpClientInterface> createHttpClient(std::string apiUri, OID snapshotId);

}  // namespace queryable

}  // namespace mongo
