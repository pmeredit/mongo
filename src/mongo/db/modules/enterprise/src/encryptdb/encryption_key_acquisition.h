/*
 * Copyright (C) 2015-present MongoDB, Inc. and subject to applicable commercial license.
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
 * Acquires the master key 'keyId' from a KMIP server and starts a periodic
 * job to routinely check the state of the key.
 *
 * If 'keyId' is empty a new key will be created.
 *
 * If 'ignoreStateAttribute' is enabled, we don't care about the state of the
 * key because we are rotating off of that key.
 */
StatusWith<std::unique_ptr<SymmetricKey>> getKeyFromKMIPServer(const KMIPParams& kmipParams,
                                                               StringData keyId,
                                                               bool ignoreStateAttribute = false);
}  // namespace mongo
