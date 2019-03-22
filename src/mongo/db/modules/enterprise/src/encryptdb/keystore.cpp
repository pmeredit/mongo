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

    iterator find(const SymmetricKeyId& keyId) override {
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
    // TODO This error message will make sense when SERVER-40074 is done, because the encryption
    // key manager will transparently upgrade the keystore from v0 to v1 on database key rollover
    // with GCM enabled.
    severe() << "The encrypted storage engine must be configured with AES256-GCM mode to "
             << "support database key rollover";
    fassertFailedNoTrace(51168);
}

std::unique_ptr<Keystore::Session> KeystoreImplV0::makeSession() {
    return stdx::make_unique<SessionImplV0>(_datastore.makeSession());
}

class KeystoreImplV1 final : public Keystore {
public:
    explicit KeystoreImplV1(WTDataStore datastore,
                            uint32_t curRolloverId,
                            StringMap<SymmetricKeyId::id_type> dbNameToKeyId)
        : _datastore(std::move(datastore)),
          _rolloverId(curRolloverId),
          _dbNameToKeyId(std::move(dbNameToKeyId)) {}

    class SessionImplV1;
    static std::unique_ptr<Keystore> makeKeystore(const boost::filesystem::path& path,
                                                  const EncryptionGlobalParams* params);

    std::unique_ptr<Session> makeSession() override;
    void rollOverKeys() override;

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
    stdx::mutex _dbNameToKeyIdMutex;
    StringMap<SymmetricKeyId::id_type> _dbNameToKeyId;
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

    iterator find(const SymmetricKeyId& keyId) override {
        // If we have a numeric key id, then just do a search in WT
        if (keyId.id()) {
            return iterator(dataStoreSession()->search(keyId.id().get()), this);
        }

        // Otherwise we need to lookup the key ID in our cache of name to ID.
        stdx::lock_guard<stdx::mutex> lk(_parent->_dbNameToKeyIdMutex);
        const auto keyIdIt = _parent->_dbNameToKeyId.find(keyId.name());
        if (keyIdIt == _parent->_dbNameToKeyId.end()) {
            return iterator();
        }

        return iterator(dataStoreSession()->search(keyIdIt->second), this);
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
        stdx::lock_guard<stdx::mutex> lk(_parent->_dbNameToKeyIdMutex);
        bool inserted;
        const auto& keyId = key->getKeyId();
        std::tie(std::ignore, inserted) =
            _parent->_dbNameToKeyId.insert({keyId.name(), keyId.id().get()});

        LOG(1) << "Cached encryption key mapping: " << view.database << " -> " << keyId.id().get();
        fassert(51167, inserted);
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
    KeystoreImplV1* _parent;
};

std::unique_ptr<Keystore> KeystoreImplV1::makeKeystore(const boost::filesystem::path& path,
                                                       const EncryptionGlobalParams* params) {
    WTDataStore keystore(path, params);

    auto session = keystore.makeSession();
    auto cursor = session.rbegin();

    StringMap<SymmetricKeyId::id_type> dbNameToKeyId;
    uint32_t rolloverId = 0;

    // If we don't have any schema yet, create the table and checkpoint the session
    if (cursor == session.end()) {
        keystore.createTable(session, kWTTableConfig);
        session.checkpoint();
    } else {
        // Otherwise walk the keystore in reverse to find all the key ID's for the current
        // rollover and figure out the max key id and rollover id.
        do {
            KeystoreRecordViewV1 view(cursor);
            rolloverId = view.rolloverId;
            bool inserted;
            std::tie(std::ignore, inserted) =
                dbNameToKeyId.insert({view.database.toString(), view.id});
            LOG(1) << "Cached encryption key mapping: " << view.database << " -> " << view.id;
            fassert(51166, inserted);
        } while ((++cursor != session.rend()) &&
                 (KeystoreRecordViewV1(cursor).rolloverId == rolloverId));
    }

    return stdx::make_unique<KeystoreImplV1>(
        std::move(keystore), rolloverId, std::move(dbNameToKeyId));
}

void KeystoreImplV1::rollOverKeys() {
    stdx::lock_guard<stdx::mutex> lk(_dbNameToKeyIdMutex);

    _rolloverId++;
    _dbNameToKeyId.clear();
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
