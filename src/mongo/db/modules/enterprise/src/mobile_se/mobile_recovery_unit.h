/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "mobile_session.h"
#include "mobile_session_pool.h"
#include "mongo/base/checked_cast.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/snapshot.h"

namespace mongo {

class SortedDataInterface;

class MobileRecoveryUnit final : public RecoveryUnit {
public:
    MobileRecoveryUnit(MobileSessionPool* sessionPool);
    virtual ~MobileRecoveryUnit();

    void beginUnitOfWork(OperationContext* opCtx) override;
    void commitUnitOfWork() override;
    void abortUnitOfWork() override;

    bool waitUntilDurable() override {
        return true;
    }

    void abandonSnapshot() override;

    void registerChange(Change* change) override;

    void* writingPtr(void* data, size_t len) override {
        MONGO_UNREACHABLE;
    }

    void setRollbackWritesDisabled() override {}

    SnapshotId getSnapshotId() const override {
        return SnapshotId();
    }

    MobileSession* getSession(OperationContext* opCtx);

    MobileSession* getSessionNoTxn(OperationContext* opCtx);

    bool inActiveTxn() const {
        return _active;
    }

    void assertInActiveTxn() const;

    void enqueueFailedDrop(std::string& dropQuery);

    static MobileRecoveryUnit* get(OperationContext* opCtx) {
        return checked_cast<MobileRecoveryUnit*>(opCtx->recoveryUnit());
    }

private:
    void _abort();
    void _commit();

    void _ensureSession(OperationContext* opCtx);
    void _txnClose(bool commit);
    void _txnOpen(OperationContext* opCtx);

    bool _areWriteUnitOfWorksBanned = false;
    bool _inUnitOfWork;
    bool _active;

    std::string _path;
    MobileSessionPool* _sessionPool;
    std::unique_ptr<MobileSession> _session;

    using Changes = std::vector<std::unique_ptr<Change>>;
    Changes _changes;
};

}  // namespace mongo
