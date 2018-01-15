/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <set>

#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/platform/basic.h"

#include "mobile_sqlite_statement.h"

#pragma once

namespace mongo {

class MobileIndex : public SortedDataInterface {
public:
    MobileIndex(OperationContext* opCtx, const IndexDescriptor* desc, const std::string& ident);

    MobileIndex(bool isUnique, const Ordering& ordering, const std::string& ident);

    virtual ~MobileIndex() {}

    Status insert(OperationContext* opCtx,
                  const BSONObj& key,
                  const RecordId& recId,
                  bool dupsAllowed) override;

    void unindex(OperationContext* opCtx,
                 const BSONObj& key,
                 const RecordId& recId,
                 bool dupsAllowed) override;

    void fullValidate(OperationContext* opCtx,
                      long long* numKeysOut,
                      ValidateResults* fullResults) const override;

    bool appendCustomStats(OperationContext* opCtx,
                           BSONObjBuilder* output,
                           double scale) const override;

    long long getSpaceUsedBytes(OperationContext* opCtx) const override;

    long long numEntries(OperationContext* opCtx) const override;

    bool isEmpty(OperationContext* opCtx) override;

    Status initAsEmpty(OperationContext* opCtx) override;

    Status dupKeyCheck(OperationContext* opCtx, const BSONObj& key, const RecordId& recId) override;

    // Beginning of MobileIndex-specific methods

    /**
     * Creates a SQLite table suitable for a new Mobile index.
     */
    static Status create(OperationContext* opCtx, const std::string& ident);

    /**
     * Performs the insert into the table with the given key and value.
     */
    template <typename ValueType>
    Status doInsert(OperationContext* opCtx,
                    const KeyString& key,
                    const ValueType& value,
                    bool isTransactional = true);

    Ordering getOrdering() const {
        return _ordering;
    }

    KeyString::Version getKeyStringVersion() const {
        return _keyStringVersion;
    }

    bool isUnique() {
        return _isUnique;
    }

    std::string getIdent() const {
        return _ident;
    }

protected:
    bool _isDup(OperationContext* opCtx, const BSONObj& key, RecordId recId);

    Status _dupKeyError(const BSONObj& key);

    /**
     * Checks if key size is too long.
     */
    static Status _checkKeySize(const BSONObj& key);

    /**
     * Performs the deletion from the table matching the given key.
     */
    void _doDelete(OperationContext* opCtx, const KeyString& key, KeyString* value = nullptr);

    virtual Status _insert(OperationContext* opCtx,
                           const BSONObj& key,
                           const RecordId& recId,
                           bool dupsAllowed) = 0;

    virtual void _unindex(OperationContext* opCtx,
                          const BSONObj& key,
                          const RecordId& recId,
                          bool dupsAllowed) = 0;

    class BulkBuilderBase;
    class BulkBuilderStandard;
    class BulkBuilderUnique;

    const bool _isUnique;
    const Ordering _ordering;
    const KeyString::Version _keyStringVersion = KeyString::kLatestVersion;
    const std::string _ident;
};

class MobileIndexStandard final : public MobileIndex {
public:
    MobileIndexStandard(OperationContext* opCtx,
                        const IndexDescriptor* desc,
                        const std::string& ident);

    MobileIndexStandard(const Ordering& ordering, const std::string& ident);

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                           bool isForward) const override;

protected:
    Status _insert(OperationContext* opCtx,
                   const BSONObj& key,
                   const RecordId& recId,
                   bool dupsAllowed) override;

    void _unindex(OperationContext* opCtx,
                  const BSONObj& key,
                  const RecordId& recId,
                  bool dupsAllowed) override;
};

class MobileIndexUnique final : public MobileIndex {
public:
    MobileIndexUnique(OperationContext* opCtx,
                      const IndexDescriptor* desc,
                      const std::string& ident);

    MobileIndexUnique(const Ordering& ordering, const std::string& ident);

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                           bool isForward) const override;

protected:
    Status _insert(OperationContext* opCtx,
                   const BSONObj& key,
                   const RecordId& recId,
                   bool dupsAllowed) override;

    void _unindex(OperationContext* opCtx,
                  const BSONObj& key,
                  const RecordId& recId,
                  bool dupsAllowed) override;

    const bool _isPartial = false;
};
}  // namespace mongo
