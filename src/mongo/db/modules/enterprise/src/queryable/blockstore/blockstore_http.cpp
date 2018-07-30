/**
 * Copyright (C) 2018 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "blockstore_http.h"

#include "mongo/util/mongoutils/str.h"

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
            status = buf.write(result.getCursor(), 0);
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

    const std::string url = str::stream() << "http://" << _apiUrl
                                          << "/os_list?snapshotId=" << _snapshotId;

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
