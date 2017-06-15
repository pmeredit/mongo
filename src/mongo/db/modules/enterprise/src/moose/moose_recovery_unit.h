/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "mongo/base/checked_cast.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/snapshot.h"
#include "moose_session.h"
#include "moose_session_pool.h"

namespace mongo {

class SortedDataInterface;

class MooseRecoveryUnit final : public RecoveryUnit {
public:
    MooseRecoveryUnit(MooseSessionPool* sessionPool);
    virtual ~MooseRecoveryUnit();

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

    MooseSession* getSession(OperationContext* opCtx);

    MooseSession* getSessionNoTxn(OperationContext* opCtx);

    bool inActiveTxn() const {
        return _active;
    }

    void assertInActiveTxn() const;

    static MooseRecoveryUnit* get(OperationContext* opCtx) {
        return checked_cast<MooseRecoveryUnit*>(opCtx->recoveryUnit());
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
    MooseSessionPool* _sessionPool;
    std::unique_ptr<MooseSession> _session;

    using Changes = std::vector<std::unique_ptr<Change>>;
    Changes _changes;
};

}  // namespace mongo
