/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "http_client.h"

#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace queryable {

HttpClientBase::HttpClientBase(std::string apiUri, OID snapshotId)
    : _apiUri(std::move(apiUri)),
      _snapshotId(snapshotId),
      _authSecret(std::getenv(kSecretKeyEnvVar)) {}

std::string HttpClientBase::getSecretHeader() const {
    return str::stream() << "Secret: " << _authSecret;
}

std::string HttpClientBase::getSnapshotUrl(StringData path,
                                           std::size_t offset,
                                           std::size_t count) const {
    return str::stream() << "http://" << _apiUri << "/os_read?snapshotId=" << _snapshotId
                         << "&filename=" << path << "&offset=" << offset << "&length=" << count;
}

std::string HttpClientBase::getListDirectoryUrl() const {
    return str::stream() << "http://" << _apiUri << "/os_list?snapshotId=" << _snapshotId;
}

}  // namespace queryable
}  // namespace mongo
