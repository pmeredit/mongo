/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/platform/basic.h"

#include <vector>

#include "mongo/bson/ordering.h"
#include "mongo/db/catalog/head_manager.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/sorted_data_interface.h"

#include "artree_index.h"
#include "artree_recovery_unit.h"

namespace mongo {

class BSONObjBuilder;
class SortedDataBuilderInterface;

/**
 */
class ARTreeSortedDataImpl : public SortedDataInterface {
public:
    static const uint64_t DEFAULT_STACK_MAX = 1024;
    static const uint64_t MAX_KEYSIZE = 1018;

    ARTreeSortedDataImpl(Ordering ordering, ARTreeIndex* index);
    virtual ~ARTreeSortedDataImpl();

    static BSONObj maxKey;
    static bool maxKeyInit;

    static bool hasFieldNames(const BSONObj& obj);
    static BSONObj stripFieldNames(const BSONObj& query);

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;

    Status insert(OperationContext* opCtx,
                  const BSONObj& key,
                  const RecordId& loc,
                  bool dupsAllowed) override;

    void unindex(OperationContext* opCtx,
                 const BSONObj& key,
                 const RecordId& loc,
                 bool dupsAllowed) override;

    Status dupKeyCheck(OperationContext* opCtx, const BSONObj& key, const RecordId& loc) override;

    void fullValidate(OperationContext* opCtx,
                      bool full,
                      long long* numKeysOut,
                      BSONObjBuilder* output) const override;

    bool appendCustomStats(OperationContext* opCtx,
                           BSONObjBuilder* output,
                           double scale) const override;

    long long getSpaceUsedBytes(OperationContext* opCtx) const override;
    bool isEmpty(OperationContext* opCtx) override;
    long long numEntries(OperationContext* opCtx) const override;
    Status initAsEmpty(OperationContext* opCtx) override;

    Ordering getOrdering() const {
        return _ordering;
    }

    std::unique_ptr<Cursor> newCursor(OperationContext* opCtx, bool isForward = true) const;

private:
    Ordering _ordering;
    ARTreeIndex* _index;
    bool _unitTesting;
};

class ARTreeSortedDataBuilderImpl : public SortedDataBuilderInterface {
public:
    virtual ~ARTreeSortedDataBuilderImpl() {}

    ARTreeSortedDataBuilderImpl(OperationContext* opCtx,
                                ARTreeSortedDataImpl* artIndex,
                                bool dupsAllowed);

    Status addKey(const BSONObj& key, const RecordId& loc) override;
    void commit(bool mayInterrupt) override;

private:
    OperationContext* _opCtx;
    ARTreeSortedDataImpl* _artIndex;
    bool _dupsAllowed;
};

class ARTreeBulkBuilder : public SortedDataBuilderInterface {
public:
    virtual ~ARTreeBulkBuilder() {}

    ARTreeBulkBuilder(OperationContext* opCtx, ARTreeSortedDataImpl* artIndex, bool dupsAllowed);

    Status addKey(const BSONObj& key, const RecordId& loc) override;
    void commit(bool mayInterrupt) override;

private:
    OperationContext* _opCtx;
    ARTreeSortedDataImpl* _artIndex;
    bool _dupsAllowed;
};

class ARTCursor : public SortedDataInterface::Cursor {
public:
    ARTCursor(OperationContext* opCtx,
              ARTreeIndex* index,
              uint32_t stackMax,
              bool isForward,
              const Ordering& ordering);

    virtual ~ARTCursor();

    /**
     * Sets the position to stop scanning. An empty key unsets the end position.
     *
     * If next() hits this position, or a seek method attempts to seek past it
     * they unposition the cursor and return boost::none.
     *
     * Setting the end position should be done before seeking since the current
     * position, if any, isn't checked.
     */
    void setEndPosition(const BSONObj& key, bool inclusive) override;

    /**
     * Moves forward and returns the new data or boost::none if there is no more
     * data.  If not positioned, returns boost::none.
     */
    boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) override;

    /**
     * Seeks to the provided key and returns current position.
     */
    boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                        bool inclusive,
                                        RequestedInfo parts = kKeyAndLoc) override;

    /**
     * Seeks to the position described by seekPoint and returns the current
     * position.
     */
    boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                        RequestedInfo parts = kKeyAndLoc) override;

    /**
     * Seeks to a key with a hint to the implementation that you only want exact
     * matches. If an exact match can't be found, boost::none will be returned
     * and the resulting position of the cursor is unspecified.
     */
    boost::optional<IndexKeyEntry> seekExact(const BSONObj& bsonKey,
                                             RequestedInfo parts = kKeyAndLoc) override {
        BSONObj key = ARTreeSortedDataImpl::stripFieldNames(bsonKey);
        auto kv = seek(key, true, kKeyAndLoc);
        if (kv && kv->key.woCompare(key, BSONObj(), /*considerFieldNames*/ false) == 0)
            return kv;
        return {};
    }

    /**
     * Prepares for state changes in underlying data in a way that allows the
     * cursor's current position to be restored.
     *
     * It is safe to call save multiple times in a row.
     * No other method (excluding destructor) may be called until successfully
     * restored.
     */
    void save() override;

    /**
     * Prepares for state changes in underlying data without necessarily saving
     * the current state.
     *
     * The cursor's position when restored is unspecified. Caller is expected to
     * seek following the restore.
     *
     * It is safe to call saveUnpositioned multiple times in a row.
     * No other method (excluding destructor) may be called until successfully
     * restored.
     */
    void saveUnpositioned() override {
        save();
    }

    /**
     * Recovers from potential state changes in underlying data.
     *
     * If the former position no longer exists, a following call to next() will
     * return the next closest position in the direction of the scan, if any.
     *
     * This handles restoring after either save() or saveUnpositioned().
     */
    void restore() override;

    void detachFromOperationContext() override;
    void reattachToOperationContext(OperationContext* opCtx) override;

private:
    Ordering getOrdering() const {
        return _ordering;
    }
    int getDirection() const;
    bool isEOF() const;
    BSONObj getKey() const;
    RecordId getRecordId() const;

    ARTreeCursor* _artreeCursor;
    bool _isForward;
    Ordering _ordering;
    OperationContext* _opCtx;
};

}  // namespace mongo
