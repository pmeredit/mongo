/**
 * Copyright (c) 2019 MongoDB, Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "keystore.h"

#include <boost/filesystem.hpp>

#include "mongo/base/string_data.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/string_map.h"
#include "symmetric_crypto.h"

namespace mongo {
namespace {
struct KeystoreRecordViewV0 {
    using WTKeyType = const char*;
    using WTValueType = std::tuple<WT_ITEM*, uint32_t, uint32_t>;

    KeystoreRecordViewV0(WTDataStoreCursor& cursor) : id(cursor.getKey<WTKeyType>()) {
        std::tie(key, status, initializationCount) =
            cursor.getValues<WT_ITEM, uint32_t, uint32_t>();
    }

    KeystoreRecordViewV0(const std::unique_ptr<SymmetricKey>& key)
        : id(key->getKeyId().name()),
          key{key->getKey(), key->getKeySize()},
          status{0},
          initializationCount{key->getInitializationCount()} {}

    WTValueType toTuple() {
        return std::make_tuple(&key, status, initializationCount);
    }

    const StringData id;
    WT_ITEM key;
    uint32_t status;
    uint32_t initializationCount;
};

struct KeystoreRecordViewV1 {
    using WTKeyType = uint64_t;
    using WTValueType = std::tuple<const char*, uint32_t, WT_ITEM*, uint32_t>;

    KeystoreRecordViewV1(WTDataStoreCursor& cursor) : id(cursor.getKey<WTKeyType>()) {
        std::tie(database, rolloverId, key, initializationCount) =
            cursor.getValues<const char*, uint32_t, WT_ITEM, uint32_t>();
    }

    KeystoreRecordViewV1(const std::unique_ptr<SymmetricKey>& key)
        : id(key->getKeyId().id().value_or(0)),
          database(key->getKeyId().name()),
          key{key->getKey(), key->getKeySize()},
          initializationCount{key->getInitializationCount()} {
        invariant(!database.empty());
    }

    WTValueType toTuple() {
        return std::make_tuple(database.rawData(), rolloverId, &key, initializationCount);
    }

    // This key ID will be assigned from WT on insert. If this view is assigned from a SymmetricKey
    // that does not have a numeric ID, this will be zero on initialization and the view will
    // be stale after the key is inserted into WT.
    const WTKeyType id;
    StringData database;
    uint32_t rolloverId;
    WT_ITEM key;
    uint32_t initializationCount;
};

}  // namespace

Keystore::Session::iterator& Keystore::Session::iterator::operator++() {
    ++_cursor;
    _curItem.reset();
    return *this;
}

UniqueSymmetricKey& Keystore::Session::iterator::operator*() {
    if (!_curItem) {
        _curItem = _session->extractSymmetricKey(_cursor);
    }
    return _curItem;
}

SymmetricKey* Keystore::Session::iterator::operator->() {
    if (!_curItem) {
        _curItem = _session->extractSymmetricKey(_cursor);
    }

    return _curItem.get();
}

bool Keystore::Session::iterator::operator==(const Keystore::Session::iterator& other) const {
    return _cursor == other._cursor;
}

bool Keystore::Session::iterator::operator!=(const Keystore::Session::iterator& other) const {
    return _cursor != other._cursor;
}

WTDataStoreCursor& Keystore::Session::iterator::cursor() {
    return _cursor;
}

class KeystoreImplV0 final : public Keystore {
public:
    explicit KeystoreImplV0(WTDataStore datastore) : _datastore(std::move(datastore)) {}

    class SessionImplV0;
    static std::unique_ptr<Keystore> makeKeystore(const boost::filesystem::path& path,
                                                  const EncryptionGlobalParams* params);

    std::unique_ptr<Session> makeSession() override;
    void rollOverKeys() override;
    std::uint32_t getRolloverId() const override;

private:
    // Create a new WT table with the following schema:
    //
    // S: database name, null-terminated string.
    // u: key material, raw byte array stored as a WT_ITEM.
    // l: key status, currently not used but set to 0.
    constexpr static auto kWTTableConfig =
        "key_format=S,value_format=uLL,columns=(keyid,key,keystatus,initializationCount)"_sd;

    WTDataStore _datastore;
};

class KeystoreImplV0::SessionImplV0 final : public Keystore::Session {
public:
    explicit SessionImplV0(WTDataStoreSession&& session)
        : Session(std::forward<WTDataStoreSession>(session)) {}

    iterator begin() override {
        return iterator(dataStoreSession()->begin(), this);
    }

    iterator end() override {
        return iterator();
    }

    // V0 keystores have no numeric IDs, therefore FindMode is meaningless.
    iterator find(const SymmetricKeyId& keyId, FindMode) override {
        return iterator(dataStoreSession()->search(keyId.name().c_str()), this);
    }

    void insert(const UniqueSymmetricKey& key) override {
        KeystoreRecordViewV0 view(key);
        dataStoreSession()->insert(view.id.rawData(), view.toTuple());
    }

    void update(iterator it, const UniqueSymmetricKey& key) override {
        KeystoreRecordViewV0 view(key);
        dataStoreSession()->update(it.cursor(), view.toTuple());
    }

protected:
    UniqueSymmetricKey extractSymmetricKey(WTDataStoreCursor& cursor) override {
        KeystoreRecordViewV0 view(cursor);
        return stdx::make_unique<SymmetricKey>(reinterpret_cast<const uint8_t*>(view.key.data),
                                               view.key.size,
                                               crypto::aesAlgorithm,
                                               view.id,
                                               view.initializationCount);
    }
};

std::unique_ptr<Keystore> KeystoreImplV0::makeKeystore(const boost::filesystem::path& path,
                                                       const EncryptionGlobalParams* params) {
    WTDataStore keystore(path, params);

    auto session = keystore.makeSession();
    auto cursor = session.begin();

    if (cursor == session.end()) {
        keystore.createTable(session, kWTTableConfig);
        session.checkpoint();
    }

    return stdx::make_unique<KeystoreImplV0>(std::move(keystore));
}

void KeystoreImplV0::rollOverKeys() {
    // EncryptionKeyManager should have upgraded to KeystoreV1 before calling rollover.
    severe() << "The encrypted storage engine must be configured with AES256-GCM mode to "
             << "support database key rollover";
    fassertFailedNoTrace(51168);
}

std::uint32_t KeystoreImplV0::getRolloverId() const {
    // Rollover is not supported in keystore V0.
    return 0;
}

std::unique_ptr<Keystore::Session> KeystoreImplV0::makeSession() {
    return stdx::make_unique<SessionImplV0>(_datastore.makeSession());
}

class KeystoreImplV1 final : public Keystore {
public:
    explicit KeystoreImplV1(WTDataStore datastore,
                            uint32_t curRolloverId,
                            StringMap<SymmetricKeyId::id_type> dbNameToKeyIdCurrent,
                            StringMap<SymmetricKeyId::id_type> dbNameToKeyIdOldest)
        : _datastore(std::move(datastore)),
          _rolloverId(curRolloverId),
          _dbNameToKeyIdCurrent(std::move(dbNameToKeyIdCurrent)),
          _dbNameToKeyIdOldest(std::move(dbNameToKeyIdOldest)) {}

    class SessionImplV1;
    static std::unique_ptr<Keystore> makeKeystore(const boost::filesystem::path& path,
                                                  const EncryptionGlobalParams* params);

    std::unique_ptr<Session> makeSession() override;
    void rollOverKeys() override;
    std::uint32_t getRolloverId() const override;

private:
    friend class SessionImplV1;

    // Create a new WT table with the following schema:
    //
    // r: key ID (uint64_t) that will be embedded in each page.
    // S: database (key) name, null-terminated string.
    // L: rollover ID - used to group keys together at startup
    // u: key material, raw byte array stored as a WT_ITEM.
    // L: initialization count, the number of times this key has been restarted
    constexpr static auto kWTTableConfig =
        "key_format=r,"
        "value_format=SLuL,"
        "columns=(keyid,database,rolloverID,key,initializationCount)"_sd;

    WTDataStore _datastore;
    uint32_t _rolloverId;

    // Looking up keys by name would require scanning the WT table, so we cache the ID's for
    // all the databases in the current rollover set.
    // *Current contains the most recent keys for a given name and is suitable for new writes.
    // *Oldest contains the original keys for a given name and are suitable for reads from v0 pages.
    stdx::mutex _dbNameToKeyIdCurrentMutex;
    StringMap<SymmetricKeyId::id_type> _dbNameToKeyIdCurrent;

    const StringMap<SymmetricKeyId::id_type> _dbNameToKeyIdOldest;
};

class KeystoreImplV1::SessionImplV1 final : public Keystore::Session {
public:
    explicit SessionImplV1(WTDataStoreSession&& session, KeystoreImplV1* parent)
        : Session(std::forward<WTDataStoreSession>(session)), _parent(parent) {}

    iterator begin() override {
        return iterator(dataStoreSession()->begin(), this);
    }

    iterator end() override {
        return iterator();
    }

    iterator find(const SymmetricKeyId& keyId, FindMode mode) override {
        const auto numericId = _findCriteriaToId(keyId, mode);
        if (!numericId) {
            return iterator();
        }

        return iterator(dataStoreSession()->search(numericId.get()), this);
    }

    void insert(const UniqueSymmetricKey& key) override {
        // Make sure the key ID of the input key has a name but hasn't been assigned an ID yet
        invariant(!key->getKeyId().name().empty());

        KeystoreRecordViewV1 view(key);
        // Set the rollover id and insert it into WT
        view.rolloverId = _parent->_rolloverId;

        if (key->getKeyId().id()) {
            dataStoreSession()->insert(view.id, view.toTuple());
        } else {
            // If we don't have a key ID already, WT will assign one for us. We then need to
            // update both the key with the correct key ID.
            SymmetricKeyId::id_type keyId = dataStoreSession()->insert(view.toTuple());
            key->setKeyId(SymmetricKeyId(view.database, keyId));
        }

        // Now that we've inserted the key, cache the mapping of dbname to numeric ID so we can
        // lookup the key by name later on.
        const auto& keyId = key->getKeyId();
        bool inserted;
        {
            stdx::lock_guard<stdx::mutex> lk(_parent->_dbNameToKeyIdCurrentMutex);
            std::tie(std::ignore, inserted) =
                _parent->_dbNameToKeyIdCurrent.insert({keyId.name(), keyId.id().get()});
        }

        // We don't worry about dbNameToKeyIdOldest mapping here because this key
        // will never have been used in a V0 page.

        LOG(1) << "Cached encryption key mapping: " << view.database << " -> " << keyId.id().get();
        fassert(51172, inserted);
    }

    void update(iterator it, const UniqueSymmetricKey& key) override {
        KeystoreRecordViewV1 view(it.cursor());
        // Check that the key is fully formed and matches the key the cursor points to
        invariant(key->getKeyId().id());
        invariant(view.id == key->getKeyId().id());
        invariant(view.database == key->getKeyId().name());

        // The ID and key data portions of the key are immutable, we just want to update the
        // initialization count in WT.
        view.initializationCount = key->getInitializationCount();

        dataStoreSession()->update(it.cursor(), view.toTuple());
    }

protected:
    UniqueSymmetricKey extractSymmetricKey(WTDataStoreCursor& cursor) override {
        KeystoreRecordViewV1 view(cursor);
        return stdx::make_unique<SymmetricKey>(reinterpret_cast<const uint8_t*>(view.key.data),
                                               view.key.size,
                                               crypto::aesAlgorithm,
                                               SymmetricKeyId(view.database, view.id),
                                               view.initializationCount);
    }

private:
    boost::optional<SymmetricKeyId::id_type> _findCriteriaToId(const SymmetricKeyId& keyId,
                                                               FindMode mode) const {
        const auto& optId = keyId.id();
        switch (mode) {
            case FindMode::kById:
                return optId;
            case FindMode::kIdOrCurrent:
                if (optId) {
                    return optId;
                }
            /* fallthrough */
            case FindMode::kCurrent: {
                stdx::lock_guard<stdx::mutex> lk(_parent->_dbNameToKeyIdCurrentMutex);
                auto it = _parent->_dbNameToKeyIdCurrent.find(keyId.name());
                if (it == _parent->_dbNameToKeyIdCurrent.end()) {
                    return {boost::none};
                }
                return it->second;
            }
            case FindMode::kIdOrOldest:
                if (optId) {
                    return optId;
                }
            /* fallthrough */
            case FindMode::kOldest: {
                auto it = _parent->_dbNameToKeyIdOldest.find(keyId.name());
                if (it == _parent->_dbNameToKeyIdOldest.end()) {
                    return {boost::none};
                }
                return it->second;
            }
        }
        MONGO_UNREACHABLE;
    }

    KeystoreImplV1* _parent;
};

std::unique_ptr<Keystore> KeystoreImplV1::makeKeystore(const boost::filesystem::path& path,
                                                       const EncryptionGlobalParams* params) {
    WTDataStore keystore(path, params);

    StringMap<SymmetricKeyId::id_type> dbNameToKeyIdCurrent, dbNameToKeyIdOldest;
    auto session = keystore.makeSession();
    uint32_t rolloverId = 0;

    // If we don't have any schema yet, create the table and checkpoint the session
    if (session.begin() == session.end()) {
        keystore.createTable(session, kWTTableConfig);
        session.checkpoint();
    } else {
        // Otherwise walk the keystore from both ends.

        // First, iterate forward to catalogue any potential v0 page layout keys.
        // If the keystore started life with v0 keys, then they'll be referenced
        // on v0 pages without their ID and need to be recovered by name.
        // v0 pages will always want the oldest version of a given key name.
        {
            auto cursor = session.begin();
            const auto initialRolloverId = KeystoreRecordViewV1(cursor).rolloverId;
            do {
                KeystoreRecordViewV1 view(cursor);
                if (view.rolloverId != initialRolloverId) {
                    break;
                }
                bool inserted;
                std::tie(std::ignore, inserted) =
                    dbNameToKeyIdOldest.insert({view.database.toString(), view.id});
                LOG(1) << "Cached possible v0 key mapping: " << view.database << " -> " << view.id;
                fassert(51167, inserted);
            } while (++cursor != session.end());
        }

        // Second, iterate backward to cache IDs for future writes.
        {
            auto cursor = session.rbegin();
            rolloverId = KeystoreRecordViewV1(cursor).rolloverId;
            do {
                KeystoreRecordViewV1 view(cursor);
                if (view.rolloverId != rolloverId) {
                    break;
                }
                bool inserted;
                std::tie(std::ignore, inserted) =
                    dbNameToKeyIdCurrent.insert({view.database.toString(), view.id});
                LOG(1) << "Cached encryption key mapping: " << view.database << " -> " << view.id;
                fassert(51133, inserted);
            } while (++cursor != session.rend());
        }
    }

    return stdx::make_unique<KeystoreImplV1>(std::move(keystore),
                                             rolloverId,
                                             std::move(dbNameToKeyIdCurrent),
                                             std::move(dbNameToKeyIdOldest));
}

void KeystoreImplV1::rollOverKeys() {
    stdx::lock_guard<stdx::mutex> lk(_dbNameToKeyIdCurrentMutex);

    _rolloverId++;
    _dbNameToKeyIdCurrent.clear();
}

std::uint32_t KeystoreImplV1::getRolloverId() const {
    return _rolloverId;
}

std::unique_ptr<Keystore::Session> KeystoreImplV1::makeSession() {
    return stdx::make_unique<SessionImplV1>(_datastore.makeSession(), this);
}

std::unique_ptr<Keystore> Keystore::makeKeystore(const boost::filesystem::path& path,
                                                 Version version,
                                                 const EncryptionGlobalParams* params) {
    switch (version) {
        case Version::k0:
            return KeystoreImplV0::makeKeystore(path, params);
        case Version::k1:
            fassert(51165, (params->encryptionCipherMode == crypto::aes256GCMName));
            return KeystoreImplV1::makeKeystore(path, params);
        default:
            MONGO_UNREACHABLE;
    }
}

std::unique_ptr<Keystore> Keystore::makeKeystore(const boost::filesystem::path& path,
                                                 const KeystoreMetadataFile& metadata,
                                                 const EncryptionGlobalParams* params) {

    return Keystore::makeKeystore(path, static_cast<Version>(metadata.getVersion()), params);
}

}  // namespace mongo
