/**
 *    Copyright (C) 2018-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <memory>
#include <ostream>
#include <string>

#include "mongo/base/status.h"
#include "mongo/platform/mutex.h"

namespace mongo {
namespace logger {

/**
 * A synchronized file output stream writer, with support for file rotation.
 *
 * To enforce proper locking, instances of RotatableFileWriter may only be manipulated by
 * instantiating a RotatableFileWriter::Use guard object, which exposes the relevant
 * manipulation methods for the stream.  For any instance of RotatableFileWriter, at most one
 * fully constructed instance of RotatableFileWriter::Use exists at a time, providing mutual
 * exclusion.
 *
 * Behavior is undefined if two instances of RotatableFileWriter should simultaneously have the
 * same value for their fileName.
 */
class RotatableFileWriter {
public:
    /**
     * Guard class representing synchronous use of an instance of RotatableFileWriter.
     */
    class Use {
        Use(const Use&) = delete;
        Use& operator=(const Use&) = delete;

    public:
        /**
         * Constructs a Use object for "writer", and lock "writer".
         */
        explicit Use(RotatableFileWriter* writer);

        /**
         * Sets the name of the target file to which stream() writes to "name".
         *
         * May be called repeatedly.
         *
         * If this method does not return Status::OK(), it is not safe to call rotate() or
         * stream().
         *
         * Set "append" to true to open "name" in append mode.  Otherwise, it is truncated.
         *
         * Set "reopen" to false to close the current stream without reopening it for the
         * new file name.
         */
        Status setFileName(const std::string& name, bool append, bool reopen = true);

        /**
         * Rotates the currently opened file into "renameTarget", and open a new file
         * with the name previously set via setFileName().
         *
         * renameFile - true we rename the log file, false we expect it was renamed externally
         *
         * append - true we open the log file in append mode, false it is truncated
         *
         * Returns Status::OK() on success.  If the rename fails, returns
         * ErrorCodes::FileRenameFailed, and the stream continues to write to the unrotated
         * file.  If the rename succeeds but the subsequent file open fails, returns
         * ErrorCodes::FileNotOpen, and the stream continues to target the original file, though
         * under its new name.
         */
        Status rotate(bool renameFile,
                      const std::string& renameTarget,
                      bool append,
                      std::function<void(Status)> onMinorError);

        /**
         * Returns the status of the stream.
         *
         * One of Status::OK(), ErrorCodes::FileNotOpen and ErrorCodes::FileStreamFailed.
         */
        Status status();

        /**
         * Returns a reference to the std::ostream() through which users may write to the file.
         */
        std::ostream& stream() {
            return *_writer->_stream;
        }

    private:
        /**
         * Helper that opens the file named by setFileName(), in the mode specified by "append".
         *
         * Returns Status::OK() on success and ErrorCodes::FileNotOpen on failure.
         */
        Status _openFileStream(bool append);

        RotatableFileWriter* _writer;
        stdx::unique_lock<Latch> _lock;
    };

private:
    friend class RotatableFileWriter::Use;
    Mutex _mutex = MONGO_MAKE_LATCH("RotatableFileWriter::_mutex");
    std::string _fileName;
    std::unique_ptr<std::ostream> _stream;
};

}  // namespace logger
}  // namespace mongo
