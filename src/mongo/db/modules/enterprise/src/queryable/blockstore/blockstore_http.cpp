/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#include "blockstore_http.h"

#include "mongo/util/str.h"

namespace mongo {
namespace queryable {

namespace {
const std::array<int, 7> kBackoffSleepSecondsRead{1, 5, 10, 15, 20, 25, 30};
const std::array<int, 3> kBackoffSleepSecondsList{1, 10, 30};
}  // namespace

BlockstoreHTTP::BlockstoreHTTP(StringData apiUrl,
                               mongo::OID snapshotId,
                               std::unique_ptr<HttpClient> client)
    : _apiUrl(apiUrl.toString()), _snapshotId(std::move(snapshotId)), _client(std::move(client)) {
    if (!_client) {
        _client = HttpClient::create();
    }
    _client->allowInsecureHTTP(true);
    _client->setHeaders({str::stream() << "Secret: " << std::getenv(kSecretKeyEnvVar)});
}

StatusWith<std::size_t> BlockstoreHTTP::read(StringData path,
                                             DataRange buf,
                                             std::size_t offset,
                                             std::size_t count) const {
    Status status(ErrorCodes::InternalError, "BlockstoreHTTP::read() returned an invalid error");

    const std::string url = str::stream()
        << "http://" << _apiUrl << "/os_read?snapshotId=" << _snapshotId << "&filename=" << path
        << "&offset=" << offset << "&length=" << count;

    for (const auto& secs : kBackoffSleepSecondsRead) {
        try {
            auto result = _client->get(url);
            status = buf.writeNoThrow(result.getCursor(), 0);
            if (status.isOK()) {
                return result.size();
            }
        } catch (...) {
            status = exceptionToStatus();
        }
        sleepsecs(secs);
    }

    return status;
}

StatusWith<DataBuilder> BlockstoreHTTP::listDirectory() const {
    Status status(ErrorCodes::InternalError,
                  "BlockstoreHTTP::listDirectory() returned an invalid error");

    const std::string url = str::stream()
        << "http://" << _apiUrl << "/os_list?snapshotId=" << _snapshotId;

    for (const auto& secs : kBackoffSleepSecondsList) {
        try {
            return _client->get(url);
        } catch (...) {
            status = exceptionToStatus();
        }
        sleepsecs(secs);
    }

    return status;
}

}  // namespace queryable
}  // namespace mongo
