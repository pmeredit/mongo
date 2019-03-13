/**
 * Copyright (c) 2019 MongoDB, Inc.
 */

#pragma once

#include <memory>
#include <tuple>
#include <utility>

#include <boost/filesystem/path.hpp>
#include <wiredtiger.h>

#include "encryption_options.h"
#include "mongo/base/string_data.h"

namespace mongo {
// A helper utility that uasserts if the Wired Tiger return code is not-okay.
void uassertWTOK(int ret);

// This is a stateless deleter for Wired Tiger structs. Wired Tiger structs have a close() method
// that takes a pointer to the object being closed, and sometimes a char* to a config string. This
// just wraps both of those up in some templates.
struct WTDeleter {
    template <
        typename Obj,
        typename std::enable_if_t<std::is_invocable_r_v<void, decltype(Obj::close), Obj*, char*>,
                                  int> = 0>
    void operator()(Obj* ptr) {
        if (ptr) {
            ptr->close(ptr, nullptr);
        }
    }

    template <
        typename Obj,
        typename std::enable_if_t<std::is_invocable_r_v<void, decltype(Obj::close), Obj*>, int> = 0>
    void operator()(Obj* ptr) {
        if (ptr) {
            ptr->close(ptr);
        }
    }
};

using UniqueWTConnection = std::unique_ptr<WT_CONNECTION, WTDeleter>;
using UniqueWTSession = std::unique_ptr<WT_SESSION, WTDeleter>;
using UniqueWTCursor = std::unique_ptr<WT_CURSOR, WTDeleter>;

class WTDataStore;
class WTDataStoreSession;

constexpr static auto kKeystoreTableName = "table:keystore"_sd;

/*
 * This is an iterator-like interface to a Wired Tiger cursor.
 *
 * They are produced by WTDataStoreSession's either be calling begin(), search(), or beginBackup().
 *
 * You can iterate over records in a for loop the way you would with an iterator:
 *
 * for (auto it = session.begin() it != session.end(); ++it) {
 *     auto key = it.getKey<KeyType>();
 *     ValueType1 value1;
 *     ValueType2 value2;
 *     std::tie(value1, value2) = it.getValues<ValueType1, ValueType2>();
 * }
 *
 * If an iterator reaches the end, it releases the underlying cursor and will equal the end()
 * iterator of the session.
 */
class WTDataStoreCursor {
public:
    // This type is copyable - it will duplicate the cursor so the copy will have its own cursor
    // state.
    WTDataStoreCursor(WTDataStoreCursor& other) : _cursor(_duplicate(other._cursor)) {}
    WTDataStoreCursor& operator=(WTDataStoreCursor& other) {
        _cursor = _duplicate(other._cursor);
        return *this;
    }

    // It's also movable
    WTDataStoreCursor(WTDataStoreCursor&&) = default;
    WTDataStoreCursor& operator=(WTDataStoreCursor&&) = default;

    // A default-constructed cursor is the same as session.end() - it's really just making
    // the UniqueWTCursor == nullptr.
    WTDataStoreCursor() = default;

    WTDataStoreCursor& operator++();

    // Returns the key of the row the cursor is currently pointing at.
    //
    // All pointer return values are unowned.
    template <typename Key>
    Key getKey() {
        invariant(_cursor);
        Key ret;
        uassertWTOK(_cursor->get_key(_cursor.get(), &ret));
        return ret;
    }

    template <typename Key,
              typename std::enable_if_t<std::is_same_v<std::remove_const_t<Key>, char*>> = 0>
    StringData getKey() {
        invariant(_cursor);
        char* ret;
        uassertWTOK(_cursor->get_key(_cursor.get(), &ret));
        return StringData(ret);
    }

    // Returns the current value(s) the cursor is currently pointing at.
    //
    // All pointers returned are unowned. They must not be referenced after the cursor is advanced.
    template <typename... Args>
    std::tuple<Args...> getValues() {
        invariant(_cursor);
        std::tuple<Args...> storage;
        auto ptrTuple = _makePointerTuple(storage);
        auto cursorTuple = std::make_tuple<WT_CURSOR*>(_cursor.get());
        uassertWTOK(std::apply(_cursor->get_value, std::tuple_cat(cursorTuple, ptrTuple)));
        return storage;
    }


    bool operator==(const WTDataStoreCursor& other) const;
    bool operator!=(const WTDataStoreCursor& other) const;

    // Releases the cursor so it does not get cleaned up when the iterator goes out of scope.
    // This is mainly used by the backup cursors so we only have to keep their session around
    // to keep the cursor alive.
    void release();

    void close();

protected:
    friend class WTDataStoreSession;
    enum class CursorDirection { kForward, kReverse };
    explicit WTDataStoreCursor(UniqueWTCursor cursor,
                               CursorDirection direction = CursorDirection::kForward)
        : _cursor(std::move(cursor)), _direction(direction) {}

    WT_CURSOR* operator->() const {
        return _cursor.get();
    }

    operator WT_CURSOR*() const {
        return _cursor.get();
    }

    // Advances the cursor by one and returns true if the advanced cursor points to a valid row
    // or false if the cursor is exhausted.
    bool advance();

private:
    // These two template functions are used to unpack values from Wired Tiger.
    //
    // makePointerTuple() takes a tuple<Args...> and returns a tuple where each type in Args is
    // converted to a pointer to the corresponding element in the source tuple.
    //
    // This way variadic C functions that expect a list of arguments of pointers to storage to
    // store their output in (e.g. Wired Tiger) can cleanly interface with C++
    template <typename... Args, std::size_t... Idx>
    auto _makePointerTuple(std::tuple<Args...>& t, std::index_sequence<Idx...>)
        -> std::tuple<typename std::add_pointer<Args>::type...> {
        return std::make_tuple(&std::get<Idx>(t)...);
    }

    template <typename... Args>
    auto _makePointerTuple(std::tuple<Args...>& t)
        -> std::tuple<typename std::add_pointer<Args>::type...> {
        return _makePointerTuple(t, std::index_sequence_for<Args...>());
    }

    UniqueWTCursor _duplicate(const UniqueWTCursor& other);

    UniqueWTCursor _cursor;
    CursorDirection _direction = CursorDirection::kForward;
};

/*
 * This class represents a WiredTiger session. It creates cursors, inserts/updates data,
 * and checkpoints (flushes) data to disk. Currently all non-backup cursors access a single
 * table called "keystore".
 */
class WTDataStoreSession {
    WTDataStoreSession(WTDataStoreSession&) = delete;
    WTDataStoreSession& operator=(WTDataStoreSession&) = delete;

public:
    using iterator = WTDataStoreCursor;

    WTDataStoreSession() = default;
    WTDataStoreSession(WTDataStoreSession&&) = default;
    WTDataStoreSession& operator=(WTDataStoreSession&&) = default;

    void close();

    // Returns an iterator (cursor) to the first row of the table.
    iterator begin();

    // Returns an invalid iterator.
    iterator end() {
        return iterator();
    }

    iterator rbegin();
    iterator rend() {
        return iterator();
    }

    // Returns an iterator that has been advanced to the row with a specific key, or end() if
    // no key exists.
    template <typename Key>
    iterator search(Key key) {
        iterator it(_makeCursor(kKeystoreTableName));
        it->set_key(it, key);

        auto ret = it->search(it);
        if (ret == WT_NOTFOUND) {
            return iterator();
        }

        uassertWTOK(ret);
        return it;
    }

    // Returns a backup cursor. The rows in this cursor have no values, and the keys are strings
    // that represent paths to files that should be included in the backup.
    iterator beginBackup();

    // Inserts a new row at Key with a tuple of args.
    template <typename Key, typename... Args>
    void insert(Key&& key, std::tuple<Args...>&& args) {
        iterator it(_makeCursor(kKeystoreTableName));
        it->set_key(it, key);

        auto cursorTuple = std::make_tuple<WT_CURSOR*>(it);
        std::apply(it->set_value, std::tuple_cat(cursorTuple, args));
        uassertWTOK(it->insert(it));
    }

    // This inserts a new row with the value contained in the tuple of args. The table must have
    // been configured with the "r" record id type as it's key. This function will return the
    // new record id number that was assigned by WT during the insert.
    template <typename... Args>
    uint64_t insert(std::tuple<Args...>&& args) {
        iterator it(_makeCursor(kKeystoreTableName, "append=true"));

        auto cursorTuple = std::make_tuple<WT_CURSOR*>(it);
        std::apply(it->set_value, std::tuple_cat(cursorTuple, args));
        uassertWTOK(it->insert(it));
        return it.getKey<uint64_t>();
    }

    // Updates or inserts a row at Key with a tuple of args.
    template <typename Key, typename... Args, typename std::enable_if_t<std::is_trivial_v<Key>> = 0>
    void update(Key&& key, std::tuple<Args...>&& args) {
        iterator it(_makeCursor(kKeystoreTableName));
        it->set_key(it, key);
        update(it, std::forward<std::tuple<Args...>>(args));
    }

    // Updates a row specified by a cursor (the cursor must already be at the correct key) with
    // a tuple of args.
    template <typename... Args>
    void update(const iterator& cursor, std::tuple<Args...> values) {
        auto cursorTuple = std::make_tuple<WT_CURSOR*>(cursor);
        std::apply(cursor->set_value, std::tuple_cat(cursorTuple, values));
        uassertWTOK(cursor->update(cursor));
    }

    // Flushes all changes made by this session to disk.
    void checkpoint() {
        uassertWTOK(_session->checkpoint(_session.get(), nullptr));
    }

protected:
    friend class WTDataStore;
    WTDataStoreSession(UniqueWTSession session) : _session(std::move(session)) {}

    WT_SESSION* operator->() const {
        return _session.get();
    }

    operator WT_SESSION*() const {
        return _session.get();
    }

private:
    UniqueWTCursor _makeCursor(StringData uri, StringData cursorOpts = {});
    UniqueWTSession _session;
};

/**
 * This represents a Wired Tiger connection.
 *
 * WTDataStoreSession/WTDataStoreCursor assume this will remain in scope for the duration of their
 * lifetime.
 */
class WTDataStore {
    WTDataStore(WTDataStore&) = delete;
    WTDataStore& operator=(WTDataStore&) = delete;

public:
    WTDataStore(const boost::filesystem::path& path,
                const EncryptionGlobalParams* const encryptionParams);
    WTDataStore() = default;

    WTDataStore(WTDataStore&&) = default;
    WTDataStore& operator=(WTDataStore&&) = default;

    void close();
    WTDataStoreSession makeSession();
    void createTable(const WTDataStoreSession& session, StringData config);

private:
    std::string _keystoreConfig;
    UniqueWTConnection _connection;
};

}  // namespace mongo
