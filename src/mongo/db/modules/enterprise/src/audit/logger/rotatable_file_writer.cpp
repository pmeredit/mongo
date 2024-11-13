/**
 *    Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "mongo/platform/basic.h"

#include "rotatable_file_writer.h"

#include <boost/filesystem/operations.hpp>
#include <cstdio>
#include <fmt/format.h>
#include <fstream>

#include "mongo/base/string_data.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/str.h"

namespace mongo {
MONGO_FAIL_POINT_DEFINE(auditLogRotateFileExists);
namespace logger {

using namespace fmt::literals;

#ifdef _WIN32
namespace {

/**
 * Converts UTF-8 encoded "utf8Str" to std::wstring.
 */
std::wstring utf8ToWide(StringData utf8Str) {
    if (utf8Str.empty()) {
        return std::wstring();
    }

    // A Windows wchar_t encoding of a unicode codepoint never takes more instances of wchar_t
    // than the UTF-8 encoding takes instances of char.
    std::unique_ptr<wchar_t[]> tempBuffer(new wchar_t[utf8Str.size()]);
    tempBuffer[0] = L'\0';
    int finalSize = MultiByteToWideChar(CP_UTF8,            // Code page
                                        0,                  // Flags
                                        utf8Str.rawData(),  // Input string
                                        utf8Str.size(),     // Count
                                        tempBuffer.get(),   // UTF-16 output buffer
                                        utf8Str.size()      // Buffer size in wide characters
    );
    // TODO(schwerin): fassert finalSize > 0?
    return std::wstring(tempBuffer.get(), finalSize);
}


/**
 * Minimal implementation of a std::streambuf for writing to Win32 files via HANDLEs.
 *
 * We require this implementation and the std::ostream subclass below to handle the following:
 * (1) Opening files for shared-delete access, so that open file handles may be renamed.
 * (2) Opening files with non-ASCII characters in their names.
 */
class Win32FileStreambuf : public std::streambuf {
    Win32FileStreambuf(const Win32FileStreambuf&) = delete;
    Win32FileStreambuf& operator=(const Win32FileStreambuf&) = delete;

public:
    Win32FileStreambuf() : _fileHandle(INVALID_HANDLE_VALUE) {}
    virtual ~Win32FileStreambuf() {
        if (is_open()) {
            CloseHandle(_fileHandle);  // TODO(schwerin): Should we check for failure?
        }
    }

    bool open(StringData fileName, bool append);
    bool is_open() {
        return _fileHandle != INVALID_HANDLE_VALUE;
    }

private:
    virtual std::streamsize xsputn(const char* s, std::streamsize count);
    virtual int_type overflow(int_type ch = traits_type::eof());

    HANDLE _fileHandle;
};

/**
 * Minimal implementation of a stream to Win32 files.
 */
class Win32FileOStream : public std::ostream {
    Win32FileOStream(const Win32FileOStream&) = delete;
    Win32FileOStream& operator=(const Win32FileOStream&) = delete;

public:
    /**
     * Constructs an instance, opening "fileName" in append or truncate mode according to
     * "append".
     */
    Win32FileOStream(const std::string& fileName, bool append) : std::ostream(&_buf), _buf() {
        if (!_buf.open(fileName, append)) {
            setstate(failbit);
        }
    }

    virtual ~Win32FileOStream() {}

private:
    Win32FileStreambuf _buf;
};

bool Win32FileStreambuf::open(StringData fileName, bool append) {
    _fileHandle = CreateFileW(utf8ToWide(fileName).c_str(),         // lpFileName
                              GENERIC_WRITE,                        // dwDesiredAccess
                              FILE_SHARE_DELETE | FILE_SHARE_READ,  // dwShareMode
                              nullptr,                              // lpSecurityAttributes
                              OPEN_ALWAYS,                          // dwCreationDisposition
                              FILE_ATTRIBUTE_NORMAL,                // dwFlagsAndAttributes
                              nullptr                               // hTemplateFile
    );


    if (INVALID_HANDLE_VALUE == _fileHandle) {
        return false;
    }

    LARGE_INTEGER zero;
    zero.QuadPart = 0LL;

    if (append) {
        if (SetFilePointerEx(_fileHandle, zero, nullptr, FILE_END)) {
            return true;
        }
    } else {
        if (SetFilePointerEx(_fileHandle, zero, nullptr, FILE_BEGIN) && SetEndOfFile(_fileHandle)) {
            return true;
        }
    }

    // TODO(schwerin): Record error info?
    CloseHandle(_fileHandle);
    return false;
}

// Called when strings are written to ostream
std::streamsize Win32FileStreambuf::xsputn(const char* s, std::streamsize count) {
    DWORD totalBytesWritten = 0;

    while (count > totalBytesWritten) {
        DWORD bytesWritten;
        if (!WriteFile(_fileHandle, s, count - totalBytesWritten, &bytesWritten, nullptr)) {
            break;
        }

        totalBytesWritten += bytesWritten;
    }

    return totalBytesWritten;
}

// Overflow is called for single character writes to the ostream
Win32FileStreambuf::int_type Win32FileStreambuf::overflow(int_type ch) {
    if (ch == traits_type::eof()) {
        return ~ch;  // Returning traits_type::eof() => failure, anything else => success.
    }

    char toPut = static_cast<char>(ch);
    if (1 == xsputn(&toPut, 1)) {
        return ch;
    }

    return traits_type::eof();
}

}  // namespace
#endif

RotatableFileWriter::Use::Use(RotatableFileWriter* writer)
    : _writer(writer), _lock(writer->_mutex) {}

Status RotatableFileWriter::Use::setFileName(const std::string& name, bool append, bool reopen) {
    _writer->_fileName = name;
    _writer->_stream.reset(nullptr);
    return reopen ? _openFileStream(append) : Status::OK();
}

Status RotatableFileWriter::Use::rotate(bool renameOnRotate,
                                        const std::string& renameTarget,
                                        bool append,
                                        std::function<void(Status)> onMinorError) {
    if (_writer->_stream) {
        _writer->_stream->flush();

        if (renameOnRotate) {

            auto targetExists = [&]() -> StatusWith<bool> {
                try {
                    return boost::filesystem::exists(renameTarget);
                } catch (const boost::exception&) {
                    return exceptionToStatus();
                }
            }();

            if (!targetExists.isOK()) {
                return Status(ErrorCodes::FileRenameFailed, targetExists.getStatus().reason())
                    .withContext("Cannot verify whether destination already exists: {}"_format(
                        renameTarget));
            }

            if (targetExists.getValue() || MONGO_unlikely(auditLogRotateFileExists.shouldFail())) {
                if (onMinorError)
                    onMinorError({ErrorCodes::FileRenameFailed,
                                  "Target already exists during log rotation. "
                                  "target={}, file={}"_format(renameTarget, _writer->_fileName)});
                return Status::OK();
            }

            boost::system::error_code ec;
            boost::filesystem::rename(_writer->_fileName, renameTarget, ec);
            if (ec) {
                if (ec == boost::system::errc::no_such_file_or_directory) {
                    if (onMinorError)
                        onMinorError(
                            {ErrorCodes::FileRenameFailed,
                             "Source file was missing during log rotation. Creating a new one. "
                             "file={}"_format(_writer->_fileName)});
                } else {
                    return Status(ErrorCodes::FileRenameFailed,
                                  "Failed to rename {} to {}: {}"_format(
                                      _writer->_fileName, renameTarget, ec.message()));
                }
            }
        }
    }
    return _openFileStream(append);
}

Status RotatableFileWriter::Use::status() {
    if (!_writer->_stream) {
        return {ErrorCodes::FileNotOpen,
                str::stream() << "File \"" << _writer->_fileName << "\" not open"};
    }
    if (_writer->_stream->fail()) {
        return {ErrorCodes::FileStreamFailed,
                str::stream() << "File \"" << _writer->_fileName << "\" in failed state"};
    }
    return Status::OK();
}

Status RotatableFileWriter::Use::_openFileStream(bool append) {
#ifdef _WIN32
    std::unique_ptr<std::ostream> newStream(new Win32FileOStream(_writer->_fileName, append));
#else
    std::ios::openmode mode = std::ios::out;
    if (append) {
        mode |= std::ios::app;
    } else {
        mode |= std::ios::trunc;
    }
    std::unique_ptr<std::ostream> newStream(new std::ofstream(_writer->_fileName.c_str(), mode));
#endif

    if (newStream->fail()) {
        return {ErrorCodes::FileNotOpen, "Failed to open \"" + _writer->_fileName + "\""};
    }
    std::swap(_writer->_stream, newStream);
    return Status::OK();
}

}  // namespace logger
}  // namespace mongo
