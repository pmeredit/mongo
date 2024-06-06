/**
 * Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "blockstore_http.h"

#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kNetwork


namespace mongo {
namespace queryable {

namespace {
const std::array<int, 7> kBackoffSleepSecondsRead{1, 5, 10, 15, 20, 25, 30};
const std::array<int, 3> kBackoffSleepSecondsList{1, 10, 30};

std::string httpMethodString(HttpClient::HttpMethod method) {
    if (method == HttpClient::HttpMethod::kGET) {
        return "GET";
    } else if (method == HttpClient::HttpMethod::kPOST) {
        return "POST";
    } else if (method == HttpClient::HttpMethod::kPUT) {
        return "PUT";
    }

    MONGO_UNREACHABLE;
}

constexpr auto kDiagnosticLogLevel = 3;

void logRequest(HttpClient::HttpMethod method, const std::string& url) {
    LOGV2_DEBUG(6328800,
                kDiagnosticLogLevel,
                "BlockstoreHTTP request",
                "method"_attr = httpMethodString(method),
                "url"_attr = url);
}

void logResponse(Status status, const std::string& url) {
    if (status.isOK()) {
        LOGV2_DEBUG(6328801, kDiagnosticLogLevel, "BlockstoreHTTP OK response", "url"_attr = url);
        return;
    }

    LOGV2_DEBUG(6328802,
                kDiagnosticLogLevel,
                "BlockstoreHTTP BAD response",
                "status"_attr = status,
                "url"_attr = url);
}

}  // namespace

BlockstoreHTTP::BlockstoreHTTP(StringData apiUrl,
                               mongo::OID snapshotId,
                               std::unique_ptr<HttpClient> client)
    : _apiUrl(apiUrl.toString()), _snapshotId(std::move(snapshotId)), _client(std::move(client)) {
    if (!_client) {
        _client = HttpClient::create();
    }
    _client->allowInsecureHTTP(true);

    // The empty "Expect:" header is included for misconfigured webservers that require for the
    // header to be present.
    _client->setHeaders({"Content-Type: application/binary",
                         "Expect:",
                         str::stream() << "Secret: " << std::getenv(kSecretKeyEnvVar)});
}

StatusWith<std::size_t> BlockstoreHTTP::read(StringData path,
                                             DataRange buf,
                                             std::size_t offset,
                                             std::size_t count) const {
    Status status(ErrorCodes::InternalError, "BlockstoreHTTP::read() returned an invalid error");

    const std::string url = str::stream()
        << "http://" << _apiUrl << "/os_read?snapshotId=" << _snapshotId << "&filename=" << path
        << "&offset=" << offset << "&length=" << count;

    ScopeGuard logOKResponse([&] { logResponse(Status::OK(), url); });

    for (const auto& secs : kBackoffSleepSecondsRead) {
        logRequest(HttpClient::HttpMethod::kGET, url);

        try {
            auto result = _client->get(url);
            if (result.size() != count) {
                // Retry the read until we get the expected number of bytes back. Prior to this
                // change, we would return EAGAIN to WiredTiger to retry the read. But the
                // WT_FILE_SYSTEM interface queryable implements circumvents the WiredTiger syscall
                // retry loop. This was seen in HELP-36742, where we got less bytes than expected
                // due to a bad header.
                LOGV2(24219, "Read != Length", "read"_attr = result.size(), "length"_attr = count);
                status = {ErrorCodes::OperationFailed,
                          "BlockstoreHTTP::read() didn't return the expected number of bytes"};
                sleepsecs(secs);
                continue;
            }

            status = buf.writeNoThrow(result.getCursor(), 0);
            if (status.isOK()) {
                return result.size();
            }
        } catch (...) {
            status = exceptionToStatus();
            logResponse(status, url);
        }
        sleepsecs(secs);
    }

    logOKResponse.dismiss();
    return status;
}

StatusWith<DataBuilder> BlockstoreHTTP::write(StringData path,
                                              ConstDataRange buf,
                                              std::size_t offset,
                                              std::size_t count) const {
    Status status(ErrorCodes::InternalError, "BlockstoreHTTP::write() returned an invalid error");

    const std::string url = str::stream()
        << "http://" << _apiUrl << "/os_wt_recovery_write?snapshotId=" << _snapshotId
        << "&filename=" << path << "&offset=" << offset << "&length=" << count;

    ScopeGuard logOKResponse([&] { logResponse(Status::OK(), url); });

    for (const auto& secs : kBackoffSleepSecondsRead) {
        logRequest(HttpClient::HttpMethod::kPOST, url);

        try {
            return _client->post(url, buf);
        } catch (...) {
            status = exceptionToStatus();
            logResponse(status, url);
        }
        sleepsecs(secs);
    }

    logOKResponse.dismiss();
    return status;
}


StatusWith<DataBuilder> BlockstoreHTTP::listDirectory() const {
    Status status(ErrorCodes::InternalError,
                  "BlockstoreHTTP::listDirectory() returned an invalid error");

    const std::string url = str::stream()
        << "http://" << _apiUrl << "/os_list?snapshotId=" << _snapshotId;

    ScopeGuard logOKResponse([&] { logResponse(Status::OK(), url); });

    for (const auto& secs : kBackoffSleepSecondsList) {
        logRequest(HttpClient::HttpMethod::kGET, url);

        try {
            return _client->get(url);
        } catch (...) {
            status = exceptionToStatus();
            logResponse(status, url);
        }
        sleepsecs(secs);
    }

    logOKResponse.dismiss();
    return status;
}

StatusWith<DataBuilder> BlockstoreHTTP::openFile(StringData path) const {
    Status status(ErrorCodes::InternalError,
                  "BlockstoreHTTP::openFile() returned an invalid error");

    const std::string url = str::stream()
        << "http://" << _apiUrl << "/os_wt_recovery_open_file?snapshotId=" << _snapshotId
        << "&filename=" << path;

    ScopeGuard logOKResponse([&] { logResponse(Status::OK(), url); });

    for (const auto& secs : kBackoffSleepSecondsList) {
        logRequest(HttpClient::HttpMethod::kGET, url);

        try {
            return _client->get(url);
        } catch (...) {
            status = exceptionToStatus();
            logResponse(status, url);
        }
        sleepsecs(secs);
    }

    logOKResponse.dismiss();
    return status;
}

StatusWith<DataBuilder> BlockstoreHTTP::renameFile(StringData from, StringData to) const {
    Status status(ErrorCodes::InternalError,
                  "BlockstoreHTTP::renameFile() returned an invalid error");

    const std::string url = str::stream()
        << "http://" << _apiUrl << "/os_wt_rename_file?snapshotId=" << _snapshotId
        << "&from=" << from << "&to=" << to;

    ScopeGuard logOKResponse([&] { logResponse(Status::OK(), url); });

    for (const auto& secs : kBackoffSleepSecondsList) {
        logRequest(HttpClient::HttpMethod::kGET, url);

        try {
            return _client->get(url);
        } catch (...) {
            status = exceptionToStatus();
            logResponse(status, url);
        }
        sleepsecs(secs);
    }

    logOKResponse.dismiss();
    return status;
}

}  // namespace queryable
}  // namespace mongo
