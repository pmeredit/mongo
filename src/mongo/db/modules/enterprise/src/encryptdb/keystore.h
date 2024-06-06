/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/filesystem/path.hpp>
#include <memory>

#include "encryption_options.h"
#include "keystore_data_store.h"
#include "keystore_metadata.h"
#include "mongo/crypto/symmetric_key.h"

namespace mongo {

/*
 * This represents a keystore for the ESE. It wraps up the calls to the underlying storage
 * and maps key ID to keys across different versions of the keystore.
 *
 * The implementation returned by makeKeystore will depend on the version field in the
 * KeystoreMetadataFile.
 */
class Keystore {
public:
    virtual ~Keystore() = default;

    static std::unique_ptr<Keystore> makeKeystore(const boost::filesystem::path& path,
                                                  const KeystoreMetadataFile& metadata,
                                                  const EncryptionGlobalParams* params);

    enum class Version : int32_t { k0 = 0, k1 = 1 };
    static std::unique_ptr<Keystore> makeKeystore(const boost::filesystem::path& path,
                                                  Version version,
                                                  const EncryptionGlobalParams* params);

    class Session {
    public:
        virtual ~Session() = default;

        // This is a simple forward iterator and reads keys from a WT cursor.
        //
        // Its lifetime is tied to the session that created it - destroying the session implicitly
        // closes all its cursors.
        //
        // Inserting or updating the underlying table does not invalidate any iterators.
        class iterator {
        public:
            // This is the past-the-end cursor.
            iterator() = default;

            iterator(WTDataStoreCursor cursor, Session* session)
                : _cursor(std::move(cursor)), _session(session) {}

            iterator& operator++();
            UniqueSymmetricKey& operator*();
            SymmetricKey* operator->();
            bool operator==(const iterator& other) const;
            bool operator!=(const iterator& other) const;
            WTDataStoreCursor& cursor();

        private:
            UniqueSymmetricKey _curItem;
            WTDataStoreCursor _cursor;
            Session* _session = nullptr;
        };

        virtual iterator begin() = 0;
        virtual iterator end() = 0;

        // kById requires keyId to have a numeric ID and will retreive that ID specifically.
        // kCurrent and kOldest will ignore any numeric ID (if present) and get the
        // current/oldest version of that named key as appropriate.
        // kIdOrCurrent and kIdOrOldest will find by ID if available,
        // and fallback on kCurrent/kOldest behavior if not.
        enum class FindMode {
            kById,
            kCurrent,
            kIdOrCurrent,
            kOldest,
            kIdOrOldest,
        };
        virtual iterator find(const SymmetricKeyId& keyId, FindMode mode) = 0;

        // Inserts a new key into the keystore. The key ID of the key must have a name, but is
        // not required to have a numeric ID. If the keystore supports numeric key IDs, the key
        // will be modified to have its assigned key ID before insert returns. The rolloverId
        // argument is optional. If set, it specifies the ID of the key's rollover set. By default,
        // we use the current rollover set.
        virtual void insert(const UniqueSymmetricKey& key,
                            boost::optional<uint32_t> rolloverId = boost::none) = 0;

        virtual void update(iterator it, const UniqueSymmetricKey& key) = 0;

        WTDataStoreSession* dataStoreSession() {
            return &_session;
        }

        // Returns the rollover set ID of the entry in the keystore pointed to by the cursor.
        virtual uint32_t getRolloverId(WTDataStoreCursor& cursor) const = 0;

    protected:
        explicit Session(WTDataStoreSession&& session) : _session(std::move(session)) {}

        virtual UniqueSymmetricKey extractSymmetricKey(WTDataStoreCursor& cursor) = 0;

    private:
        WTDataStoreSession _session;
    };

    virtual std::unique_ptr<Session> makeSession() = 0;
    virtual void rollOverKeys() = 0;
    virtual std::uint32_t getRolloverId() const = 0;
    virtual bool keyStoredThisProcess(const SymmetricKeyId& keyId) const = 0;
};

}  // namespace mongo
