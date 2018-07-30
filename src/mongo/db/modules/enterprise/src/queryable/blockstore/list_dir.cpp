/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "list_dir.h"

#include <memory>
#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
namespace queryable {

StatusWith<std::vector<struct File>> listDirectory(const BlockstoreHTTP& blockstore) {
    auto swListdirResponse = blockstore.listDirectory();
    if (!swListdirResponse.isOK()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Bad HTTP response from the ApiServer: "
                              << swListdirResponse.getStatus().reason()};
    }

    auto dataPtr = swListdirResponse.getValue().release();
    auto bsonObj = BSONObj(dataPtr.get());
    bool isOk;
    auto status = bsonExtractBooleanField(bsonObj, "ok", &isOk);
    if (!status.isOK()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Bad HTTP response from the ApiServer: "
                              << std::string(dataPtr.get())};
    }

    if (!isOk) {
        std::string msg;
        if (!bsonExtractStringField(bsonObj, "message", &msg).isOK()) {
            return {ErrorCodes::OperationFailed,
                    str::stream() << "ListDir failed without a message."};
        }
        return {ErrorCodes::OperationFailed, str::stream() << "ListDir failed. Message: " << msg};
    }

    BSONElement fileListElem;
    status = bsonExtractTypedField(bsonObj, "files", mongo::Array, &fileListElem);
    if (!status.isOK()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Malformed 'files' field. Expected array. Received: "
                              << fileListElem.type()};
    }

    static_assert(sizeof(std::uint64_t) == sizeof(long long), "long long is a 64 bit int");

    std::vector<struct File> ret;
    auto fileListArr = fileListElem.Array();
    for (auto fileObj : fileListArr) {
        if (fileObj.type() != mongo::Object) {
            return {ErrorCodes::OperationFailed,
                    str::stream() << "Malformed 'files' element. Expected object. Received: "
                                  << fileObj.type()};
        }

        auto file = fileObj.Obj();
        struct File fileStruct;
        if (!bsonExtractStringField(file, "filename", &fileStruct.filename).isOK()) {
            return {ErrorCodes::OperationFailed,
                    str::stream() << "Malformed 'files' element. Filename is not a string"};
        }

        long long fileSize;
        BSONElement fileSizeElem;
        status = bsonExtractField(file, "fileSize", &fileSizeElem);
        if (!status.isOK()) {
            return {ErrorCodes::OperationFailed,
                    str::stream() << "Malformed 'fileSize' element. Message: " << status.reason()};
        }
        fileSizeElem.coerce(&fileSize);
        fileStruct.fileSize = static_cast<std::int64_t>(fileSize);
        uassert(ErrorCodes::OperationFailed,
                str::stream() << "Negative file size. File: " << fileStruct.filename,
                fileStruct.fileSize >= 0);

        std::int32_t blockSize;
        BSONElement blockSizeElem;
        status = bsonExtractField(file, "blockSize", &blockSizeElem);
        if (!status.isOK()) {
            return {ErrorCodes::OperationFailed,
                    str::stream() << "Malformed 'blockSize' element. Message: " << status.reason()};
        }
        blockSizeElem.coerce(&blockSize);
        fileStruct.blockSize = static_cast<std::int32_t>(blockSize);
        uassert(ErrorCodes::OperationFailed,
                str::stream() << "Negative block size. File: " << fileStruct.filename,
                fileStruct.blockSize >= 0);

        ret.push_back(std::move(fileStruct));
    }

    return ret;
}

}  // namespace queryable
}  // namespace mongo
