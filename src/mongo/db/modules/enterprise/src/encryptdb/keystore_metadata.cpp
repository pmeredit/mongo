/**
 *  Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include <boost/filesystem.hpp>
#include <fmt/format.h>
#include <fstream>

#include "keystore_metadata.h"
#include "mongo/bson/bson_validate.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/util/errno_util.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace {

using namespace fmt::literals;

Status makeError(StringData action,
                 const boost::filesystem::path& path,
                 ErrorCodes::Error err = ErrorCodes::InternalError) {
    auto ec = lastSystemError();
    return Status(err,
                  "Failed to {} keystore metadata file ({}): {}"_format(
                      action, path.string(), errorMessage(ec)));
}

}  // namespace

StatusWith<KeystoreMetadataFile> KeystoreMetadataFile::load(const boost::filesystem::path& path,
                                                            const UniqueSymmetricKey& key,
                                                            const EncryptionGlobalParams& params) {
    if (!boost::filesystem::exists(path)) {
        return {ErrorCodes::NonExistentPath,
                str::stream() << "Keystore metadata file " << path.string() << " not found."};
    }

    if (!boost::filesystem::is_regular_file(path)) {
        return {ErrorCodes::BadValue,
                str::stream() << "Keystore metadata file " << path.string()
                              << " must be a regular file"};
    }

    std::vector<uint8_t> rawData;
    {
        std::ifstream dataFile(path.string(), std::ios::binary);
        if (!dataFile) {
            return makeError("open", path, ErrorCodes::FileOpenFailed);
        }

        auto fileSize = boost::filesystem::file_size(path);
        if (fileSize > BSONObjMaxInternalSize) {
            return {ErrorCodes::BadValue,
                    str::stream() << "Keystore meta data file " << path.string() << " is "
                                  << fileSize << " bytes, but the maximum size is "
                                  << BSONObjMaxInternalSize};
        }

        rawData.resize(fileSize);
        dataFile.read(reinterpret_cast<char*>(rawData.data()), fileSize);
        if (!dataFile) {
            return makeError("read contents of the", path);
        }
    }

    const auto cipherMode = crypto::getCipherModeFromString(params.encryptionCipherMode);
    std::vector<uint8_t> decryptedData(
        expectedPlaintextLen(cipherMode, rawData.data(), rawData.size()).second);
    size_t outLen = 0;
    auto decryptStatus = aesDecrypt(*key,
                                    cipherMode,
                                    crypto::PageSchema::k0,
                                    rawData.data(),
                                    rawData.size(),
                                    decryptedData.data(),
                                    decryptedData.size(),
                                    &outLen);

    if (!decryptStatus.isOK()) {
        return decryptStatus.withContext("Failed to decrypt keystore metadata file");
    }

    decryptedData.resize(outLen);

    try {
        auto* dataPtr = reinterpret_cast<const char*>(decryptedData.data());
        uassertStatusOK(validateBSON(dataPtr, outLen));
        IDLParserContext ctx("ESE keystore metadata");
        return KeystoreMetadataFileData::parse(ctx, BSONObj(dataPtr));
    } catch (const DBException& e) {
        return e.toStatus().withContext("Failed to parse keystore metadata file");
    }
}

Status KeystoreMetadataFile::store(const boost::filesystem::path& path,
                                   const UniqueSymmetricKey& key,
                                   const EncryptionGlobalParams& params) {
    auto obj = toBSON();
    const auto cipherMode = crypto::getCipherModeFromString(params.encryptionCipherMode);

    // Add a constant factor for any padding/tag space used during encryption.
    // The modes have different amounts of overhead. GCM has 12(tag) + 12(IV) bytes, and CBC
    // has 16(IV) + 16(max padding overhead). Use CBC's which is larger.
    std::vector<uint8_t> encrypted(obj.objsize() + 32);
    size_t outLen = 0;

    auto status = aesEncrypt(*key,
                             cipherMode,
                             crypto::PageSchema::k0,
                             reinterpret_cast<const uint8_t*>(obj.objdata()),
                             obj.objsize(),
                             encrypted.data(),
                             encrypted.size(),
                             &outLen);
    if (!status.isOK()) {
        return status.withContext("Failed to encrypt keystore metadata file");
    }

    encrypted.resize(outLen);
    auto metadataTempPath = path.parent_path() / path.filename().replace_extension("tmp");
    {
        std::ofstream outFile(metadataTempPath.string(), std::ios::binary);
        if (!outFile) {
            return makeError("open", path);
        }

        outFile.write(reinterpret_cast<char*>(encrypted.data()), encrypted.size());
        if (!outFile) {
            return makeError("write", path);
        }

        outFile.flush();
        if (!outFile) {
            return makeError("flush", path);
        }
    }

    try {
        // This follows the same procedure to fsync/rename the file as the storage metadata file.
        // Renaming a file (at least on POSIX) should:
        // 1) fsync the temporary file.
        // 2) perform the rename.
        // 3) fsync the to and from directory (in this case, both to and from are the same).
        if (!fsyncFile(metadataTempPath)) {
            return makeError("fsync", path);
        }

        boost::filesystem::rename(metadataTempPath, path);
        flushMyDirectory(path);
    } catch (const std::exception& ex) {
        return Status(ErrorCodes::InternalError,
                      str::stream() << "Failed to flush keystore metadata file (" << path.string()
                                    << "): " << ex.what());
    }

    return Status::OK();
}

}  // namespace mongo
