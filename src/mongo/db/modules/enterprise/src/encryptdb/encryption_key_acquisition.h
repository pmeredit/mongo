/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */
#pragma once

#include <memory>

namespace mongo {

/*
 * These are the methods which provide key acquisition for the rest of the encryption subsystem.
 */

struct KMIPParams;
struct SSLParams;

template <typename T>
class StatusWith;

class StringData;
class SymmetricKey;

/**
 * Acquires the system key from the encryption keyfile.
 */
StatusWith<std::unique_ptr<SymmetricKey>> getKeyFromKeyFile(StringData encryptionKeyFile);

/**
 * Acquires the master key 'keyId' from a KMIP server.
 *
 * If 'keyId' is empty a new key will be created.
 */
StatusWith<std::unique_ptr<SymmetricKey>> getKeyFromKMIPServer(const KMIPParams& kmipParams,
                                                               const SSLParams& sslParams,
                                                               StringData keyId);
}  // namespace mongo
