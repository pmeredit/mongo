/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#pragma once

#include "boost/filesystem/path.hpp"

#include "encryption_options.h"
#include "mongo/db/modules/enterprise/src/encryptdb/keystore_metadata_gen.h"
#include "symmetric_key.h"

namespace mongo {
/*
 * This wraps up the keystore metdata in a BSON file encrypted with a SymmetricKey (usually
 * the master key).
 *
 * The keystore metadata file contains the information necessary to detect the schema of the
 * actual wiredtiger keystore tables and whether the keystore was "dirty" (e.g. had an unclean
 * shutdown).
 */
class KeystoreMetadataFile : public KeystoreMetadataFileData {
public:
    KeystoreMetadataFile(KeystoreMetadataFileData&& data)
        : KeystoreMetadataFileData(std::move(data)) {}

    KeystoreMetadataFile() : KeystoreMetadataFileData(0) {}
    explicit KeystoreMetadataFile(int version) : KeystoreMetadataFileData(version) {}

    static StatusWith<KeystoreMetadataFile> load(const boost::filesystem::path& path,
                                                 const UniqueSymmetricKey& key,
                                                 const EncryptionGlobalParams& params);
    Status store(const boost::filesystem::path& path,
                 const UniqueSymmetricKey& key,
                 const EncryptionGlobalParams& params);
};

}  // namespace mongo
