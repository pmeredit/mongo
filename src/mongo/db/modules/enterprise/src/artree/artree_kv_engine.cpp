// artree_kv_engine.cpp

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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/util/log.h"

#include "artree.h"
#include "artree_common.h"
#include "artree_cursor.h"
#include "artree_debug.h"
#include "artree_kv_engine.h"
#include "artree_oplog_store.h"
#include "artree_record_store.h"
#include "artree_recovery_unit.h"
#include "artree_sorted_data_impl.h"
#include "artree_util.h"

namespace mongo {

//
// advance global clock
//
uint64_t ARTreeKVEngine::allocTxnId(ReaderWriterEnum e) {
    uint64_t timestamp = _globalTxnId;

    if (e == en_reader)
        while (!ARTree::isReader(timestamp))
            timestamp = ++_globalTxnId;  // atomic incr
    else
        while (ARTree::isReader(timestamp))
            timestamp = ++_globalTxnId;  // atomic incr

    return timestamp;
}

ARTreeKVEngine::ARTreeKVEngine() : _globalTxnId(2) {
    memset(_timestamps, 0, sizeof(ARTreeOrderedList));
    _mutex[0] = 0;
}

RecoveryUnit* ARTreeKVEngine::newRecoveryUnit() {
    return new ARTreeRecoveryUnit(this);
}

Status ARTreeKVEngine::createRecordStore(OperationContext* opCtx,
                                         StringData ns,
                                         StringData ident,
                                         const CollectionOptions& options) {
    if (IDENT_TRACE)
        log() << "ARTreeKVEngine::" << __FUNCTION__ << ":" << __LINE__ << " ident = " << ident
              << " ns = " << ns;

    MutexSpinLock::lock(_mutex);
    _artMap[ident] = ARTree::create(this, options.cappedSize, options.cappedMaxDocs);
    MutexSpinLock::unlock(_mutex);
    return Status::OK();
}

Status ARTreeKVEngine::createSortedDataInterface(OperationContext* opCtx,
                                                 StringData ident,
                                                 const IndexDescriptor* desc) {
    MutexSpinLock::lock(_mutex);
    const ARTreeRecordStore* rs =
        dynamic_cast<const ARTreeRecordStore*>(desc->getCollection()->getRecordStore());
    _indexMap[ident] = new ARTreeIndex(rs->getART());
    MutexSpinLock::unlock(_mutex);
    return Status::OK();
}

RecordStore* ARTreeKVEngine::getRecordStore(OperationContext* opCtx,
                                            StringData ns,
                                            StringData ident,
                                            const CollectionOptions& options) {
    MutexSpinLock::lock(_mutex);
    ARTree* art = _artMap[ident];

    while (!art)
        _artMap[ident] = art = ARTree::create(this, options.cappedSize, options.cappedMaxDocs);

    StringData catalog("_mdb_catalog");
    art->_catalogCollection = (ns == catalog);
    RecordStore* store;

    if (NamespaceString::oplog(ns)) {
        log() << "ARTreeKVEngine::" << __FUNCTION__ << ":" << __LINE__ << " => OplogStore"
              << " ident = " << ident << " ns = " << ns;
        store = new ARTreeOplogStore(ns, ident, art);
    } else {
        log() << "ARTreeKVEngine::" << __FUNCTION__ << ":" << __LINE__ << " => RecordStore"
              << " ident = " << ident << " ns = " << ns;
        store = new ARTreeRecordStore(ns, ident, art);
    }
    MutexSpinLock::unlock(_mutex);
    return store;
}

Ordering ARTreeKVEngine::_getOrdering(const IndexDescriptor* desc) {
    return Ordering::make(desc->keyPattern());
}

ARTreeIndex* ARTreeKVEngine::_getIndex(StringData ident, const IndexDescriptor* desc) {
    return _indexMap[ident];
}

ARTree* ARTreeKVEngine::_getARTree(StringData ident) {
    return _artMap[ident];
}

SortedDataInterface* ARTreeKVEngine::getSortedDataInterface(OperationContext* opCtx,
                                                            StringData ident,
                                                            const IndexDescriptor* desc) {
    return new ARTreeSortedDataImpl(_getOrdering(desc), _getIndex(ident, desc));
}

bool ARTreeKVEngine::supportsDocLocking() const {
    return true;
}

bool ARTreeKVEngine::supportsDirectoryPerDB() const {
    return false;
}

Status ARTreeKVEngine::repairIdent(OperationContext* opCtx, StringData ident) {
    return Status::OK();
}

Status ARTreeKVEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    if (IDENT_TRACE)
        log() << "ARTreeKVEngine::" << __FUNCTION__ << ":" << __LINE__ << " ident = " << ident;

    MutexSpinLock::lock(_mutex);
    std::size_t a = ident.find("index-");
    if (0 == a) {  // delete index
        if (IDENT_TRACE)
            log() << "ARTreeKVEngine::" << __FUNCTION__ << ":" << __LINE__ << " - drop index";

        if (_indexMap.find(ident) == _indexMap.end())
            return Status(ErrorCodes::InternalError, "deleting non-existent index");
        _indexMap.erase(ident);
        MutexSpinLock::unlock(_mutex);

        return Status::OK();
    }

    a = ident.find("collection-");
    if (0 == a) {  // delete collection

        if (_artMap.find(ident) == _artMap.end())
            return Status(ErrorCodes::InternalError, "deleting non-existent collection");
        ARTree* art = _artMap[ident];
        _artMap.erase(ident);

        // now check if still in use (through rename)
        bool found = false;
        for (auto it = _artMap.begin(); it != _artMap.end(); ++it) {
            if (it->second == art) {
                found = true;
                break;
            }
        }

        if (IDENT_TRACE)
            log() << "ARTreeKVEngine::" << __FUNCTION__ << ":" << __LINE__ << " found = " << found
                  << " _artMap.find(" << ident << ") => " << (_artMap.find(ident) != _artMap.end());

        // if not in use, remove it
        if (!found) {
            art->_isClosed = true;
            if (art && !art->_nClients) {
                art->close();
                art = nullptr;
            }
        }

        MutexSpinLock::unlock(_mutex);
        return Status::OK();
    }

    return Status(ErrorCodes::InternalError, "ident not recognized");
}

bool ARTreeKVEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
    if (_artMap.find(ident) != _artMap.end())
        return true;
    if (_indexMap.find(ident) != _indexMap.end())
        return true;
    return false;
}

int64_t ARTreeKVEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
    auto it = _artMap.find(ident);
    if (it == _artMap.end())
        return 0;
    return it->second->getSize();
}

std::vector<std::string> ARTreeKVEngine::getAllIdents(OperationContext* opCtx) const {
    std::vector<std::string> all;
    for (auto it = _artMap.begin(); it != _artMap.end(); ++it)
        all.push_back(it->first);
    for (auto it = _indexMap.begin(); it != _indexMap.end(); ++it)
        all.push_back(it->first);
    return all;
}

int ARTreeKVEngine::flushAllFiles(bool sync) {
    return 1;
}

Status ARTreeKVEngine::okToRename(OperationContext* opCtx,
                                  StringData fromNS,
                                  StringData toNS,
                                  StringData ident,
                                  const RecordStore* originalRecordStore) const {
    return Status::OK();
}

void ARTreeKVEngine::cleanShutdown() {
    return;
}
}
