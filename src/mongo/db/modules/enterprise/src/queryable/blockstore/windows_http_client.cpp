/**
 *  Copyright (C) 2016 MongoDB Inc.
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "windows_http_client.h"

#include <wininet.h>

#include "mongo/base/init.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"

#include "mongo/util/log.h"

namespace mongo {
namespace queryable {

/**
 * Supports idempotent initialization of the WinInet library.
 */
class WinInetLibraryManager {
public:
    ~WinInetLibraryManager() {
        if (_internetHandle) {
            InternetCloseHandle(_internetHandle);
        }
    }

    void initialize() {
        PVOID lpContext = nullptr;

        std::call_once(once_control, [this]() {
            _internetHandle =
                InternetOpenA("MongoDB", INTERNET_OPEN_TYPE_DIRECT, nullptr, nullptr, 0);

            if (_internetHandle == nullptr) {
                DWORD gle = GetLastError();
                severe() << "Failed to initialize wininet.dll: Error " << errnoWithDescription(gle);
                fassertFailed(40281);
            }
        });
    }

    HINTERNET getHandle() {
        invariant(_internetHandle);
        return _internetHandle;
    }

private:
    std::once_flag once_control;
    HINTERNET _internetHandle = nullptr;
};

namespace {

WinInetLibraryManager winInetLibraryManager;
const size_t kBufferSize = 16384;

/**
 * Get the last wininet error from TLS.
 */
std::string getLastInternetError() {

    DWORD error = 0;
    DWORD bufferLength = 0;
    BOOL ret = InternetGetLastResponseInfoA(&error, nullptr, &bufferLength);

    DWORD gle = GetLastError();
    if (!(ret == FALSE && gle == ERROR_INSUFFICIENT_BUFFER)) {
        return str::stream() << "getLastInternetError failed with " << gle;
    }

    // Add space for a null-terminator.
    bufferLength += 1;
    std::unique_ptr<char[]> errorMsg(new char[bufferLength]);

    ret = InternetGetLastResponseInfoA(&error, errorMsg.get(), &bufferLength);
    if (ret != TRUE) {
        gle = GetLastError();
        return str::stream() << "getLastInternetError failed with " << gle;
    }

    return {errorMsg.get(), bufferLength};
}

}  // namespace

std::unique_ptr<HttpClientInterface> createHttpClient(std::string apiUri, OID snapshotId) {
    winInetLibraryManager.initialize();

    return stdx::make_unique<WindowsHttpClient>(std::move(apiUri), snapshotId);
}

WindowsHttpClient::WindowsHttpClient(std::string apiUri, OID snapshotId)
    : HttpClientBase(apiUri, snapshotId) {}

StatusWith<std::size_t> WindowsHttpClient::read(std::string path,
                                                DataRange buf,
                                                std::size_t offset,
                                                std::size_t count) const {
    std::string lastErr;

    std::string url(getSnapshotUrl(path, offset, count));
    std::string secretHeader(getSecretHeader());

    // TODO: revist whether the retries are needed for the Queryable backup http server
    std::vector<int> kBackoffSleepDurations{1, 5, 10, 15, 20, 25, 30};
    for (std::size_t attempt = 0; attempt < kBackoffSleepDurations.size(); ++attempt) {

        HINTERNET myHandle = InternetOpenUrlA(
            winInetLibraryManager.getHandle(),
            url.c_str(),
            secretHeader.c_str(),
            secretHeader.size(),
            // Eliminate any use of TLS, cache or redirection.
            INTERNET_FLAG_NO_AUTH | INTERNET_FLAG_KEEP_CONNECTION | INTERNET_FLAG_NO_COOKIES |
                INTERNET_FLAG_NO_AUTO_REDIRECT | INTERNET_FLAG_IGNORE_REDIRECT_TO_HTTP |
                INTERNET_FLAG_IGNORE_REDIRECT_TO_HTTPS | INTERNET_FLAG_NO_CACHE_WRITE |
                INTERNET_FLAG_NO_UI | INTERNET_FLAG_RELOAD,
            NULL);

        if (!myHandle) {
            DWORD gle = GetLastError();

            auto errorMsg = getLastInternetError();
            lastErr = str::stream() << "Bad HTTP response from API server (" << gle
                                    << "): " << errorMsg;
            sleepsecs(kBackoffSleepDurations[attempt]);
            continue;
        }

        const auto guard = MakeGuard([myHandle] { InternetCloseHandle(myHandle); });

        std::array<char, kBufferSize> buffer;
        DWORD read;

        const std::size_t kStartSize = 64 * 1024;
        DataBuilder data(kStartSize);
        BOOL ret;

        do {
            ret = InternetReadFile(myHandle, buffer.data(), kBufferSize, &read);

            if (!ret) {
                DWORD gle = GetLastError();

                auto errorMsg = getLastInternetError();
                lastErr = str::stream() << "Bad HTTP response from API server (" << gle
                                        << "): " << errorMsg;
                sleepsecs(kBackoffSleepDurations[attempt]);
                break;
            }

            uassertStatusOK(data.writeAndAdvance(ConstDataRange(buffer.data(), read)));

        } while (read != 0);

        // If we failed to read, then retry
        if (!ret) {
            continue;
        }

        uassertStatusOK(buf.write(data.getCursor()));

        return {data.size()};
    }

    return {ErrorCodes::OperationFailed, lastErr};
}

StatusWith<DataBuilder> WindowsHttpClient::listDirectory() const {
    std::string url(getListDirectoryUrl());
    std::string secretHeader(getSecretHeader());

    std::string lastErr;
    std::vector<int> kBackoffSleepDurations{1, 10, 30};
    for (std::size_t attempt = 0; attempt < kBackoffSleepDurations.size(); ++attempt) {

        HINTERNET myHandle = InternetOpenUrlA(
            winInetLibraryManager.getHandle(),
            url.c_str(),
            secretHeader.c_str(),
            secretHeader.size(),
            // Eliminate any use of TLS, cache or redirection.
            INTERNET_FLAG_NO_AUTH | INTERNET_FLAG_KEEP_CONNECTION | INTERNET_FLAG_NO_COOKIES |
                INTERNET_FLAG_NO_AUTO_REDIRECT | INTERNET_FLAG_IGNORE_REDIRECT_TO_HTTP |
                INTERNET_FLAG_IGNORE_REDIRECT_TO_HTTPS | INTERNET_FLAG_NO_CACHE_WRITE |
                INTERNET_FLAG_NO_UI | INTERNET_FLAG_RELOAD,
            NULL);

        if (!myHandle) {
            DWORD gle = GetLastError();

            auto errorMsg = getLastInternetError();
            lastErr = str::stream() << "Bad HTTP response from API server (" << gle
                                    << "): " << errorMsg;
            sleepsecs(kBackoffSleepDurations[attempt]);
            continue;
        }

        const auto guard = MakeGuard([myHandle] { InternetCloseHandle(myHandle); });

        std::array<char, kBufferSize> buffer;
        DWORD read;

        const std::size_t kStartSize = 64 * 1024;
        DataBuilder data(kStartSize);
        BOOL ret;

        do {
            ret = InternetReadFile(myHandle, buffer.data(), kBufferSize, &read);

            if (!ret) {
                DWORD gle = GetLastError();
                auto errorMsg = getLastInternetError();
                lastErr = str::stream() << "Bad HTTP response from API server (" << gle
                                        << "): " << errorMsg;
                sleepsecs(kBackoffSleepDurations[attempt]);
                continue;
            }

            uassertStatusOK(data.writeAndAdvance(ConstDataRange(buffer.data(), read)));

        } while (read != 0);

        // If we failed to read, then retry
        if (!ret) {
            continue;
        }

        return {std::move(data)};
    }

    return {ErrorCodes::OperationFailed, lastErr};
}

}  // namespace queryable
}  // namespace mongo
