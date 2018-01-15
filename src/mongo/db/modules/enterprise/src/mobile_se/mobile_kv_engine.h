/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <memory>

#include "mobile_session_pool.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/string_map.h"

namespace mongo {

class JournalListener;

class MobileKVEngine : public KVEngine {
public:
    MobileKVEngine(const std::string& path);

    RecoveryUnit* newRecoveryUnit() override;

    Status createRecordStore(OperationContext* opCtx,
                             StringData ns,
                             StringData ident,
                             const CollectionOptions& options) override;

    std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx,
                                                StringData ns,
                                                StringData ident,
                                                const CollectionOptions& options) override;

    Status createSortedDataInterface(OperationContext* opCtx,
                                     StringData ident,
                                     const IndexDescriptor* desc) override;

    SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                StringData ident,
                                                const IndexDescriptor* desc) override;

    Status beginBackup(OperationContext* opCtx) override {
        return Status::OK();
    }

    void endBackup(OperationContext* opCtx) override {}

    Status dropIdent(OperationContext* opCtx, StringData ident) override;

    bool supportsDocLocking() const override {
        return false;
    }

    bool supportsDBLocking() const override {
        return false;
    }

    bool supportsDirectoryPerDB() const override {
        return false;
    }

    bool isDurable() const override {
        return true;
    }

    /**
     * Flush is a no-op since SQLite transactions are durable by default after each commit.
     */
    int flushAllFiles(OperationContext* opCtx, bool sync) override {
        return 0;
    }

    bool isEphemeral() const override {
        return false;
    }

    int64_t getIdentSize(OperationContext* opCtx, StringData ident) override;

    Status repairIdent(OperationContext* opCtx, StringData ident) override {
        return Status::OK();
    }

    void cleanShutdown() override{};

    bool hasIdent(OperationContext* opCtx, StringData ident) const override;

    std::vector<std::string> getAllIdents(OperationContext* opCtx) const override;

    void setJournalListener(JournalListener* jl) override {
        stdx::unique_lock<stdx::mutex> lk(_mutex);
        _journalListener = jl;
    }

private:
    mutable stdx::mutex _mutex;
    void _initDBPath(const std::string& path);

    std::unique_ptr<MobileSessionPool> _sessionPool;

    // Notified when we write as everything is considered "journalled" since repl depends on it.
    JournalListener* _journalListener = &NoOpJournalListener::instance;

    std::string _path;
};
}  // namespace mongo
