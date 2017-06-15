/**
 * Copyright (C) 2017 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "moose_recovery_unit.h"

#include <string>

#include "mongo/db/operation_context.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "moose_sqlite_statement.h"
#include "moose_util.h"

namespace mongo {

MooseRecoveryUnit::MooseRecoveryUnit(MooseSessionPool* sessionPool)
    : _inUnitOfWork(false), _active(false), _sessionPool(sessionPool) {}

MooseRecoveryUnit::~MooseRecoveryUnit() {
    invariant(!_inUnitOfWork);
    _abort();
}

void MooseRecoveryUnit::_commit() {
    if (_session && _active) {
        _txnClose(true);
    }

    for (auto& change : _changes) {
        try {
            change->commit();
        } catch (...) {
            std::terminate();
        }
    }
    _changes.clear();
}

void MooseRecoveryUnit::_abort() {
    if (_session && _active) {
        _txnClose(false);
    }
    for (auto it = _changes.rbegin(); it != _changes.rend(); ++it) {
        try {
            (*it)->rollback();
        } catch (...) {
            std::terminate();
        }
    }
    _changes.clear();
    invariant(!_active);
}

void MooseRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
    invariant(!_areWriteUnitOfWorksBanned);
    invariant(!_inUnitOfWork);
    _inUnitOfWork = true;
}

void MooseRecoveryUnit::commitUnitOfWork() {
    invariant(_inUnitOfWork);
    _inUnitOfWork = false;
    _commit();
}

void MooseRecoveryUnit::abortUnitOfWork() {
    invariant(_inUnitOfWork);
    _inUnitOfWork = false;
    _abort();
}

void MooseRecoveryUnit::abandonSnapshot() {
    invariant(!_inUnitOfWork);
    if (_active) {
        // We can't be in a WriteUnitOfWork, so it is safe to rollback.
        _txnClose(false);
    }
    _areWriteUnitOfWorksBanned = false;
}

void MooseRecoveryUnit::registerChange(Change* change) {
    invariant(_inUnitOfWork);
    _changes.push_back(std::unique_ptr<Change>{change});
}

MooseSession* MooseRecoveryUnit::getSession(OperationContext* opCtx) {
    if (!_active) {
        _txnOpen(opCtx);
    }
    return _session.get();
}

MooseSession* MooseRecoveryUnit::getSessionNoTxn(OperationContext* opCtx) {
    _ensureSession(opCtx);
    return _session.get();
}

void MooseRecoveryUnit::assertInActiveTxn() const {
    fassert(37050, _active);
}

void MooseRecoveryUnit::_ensureSession(OperationContext* opCtx) {
    if (!_session) {
        _session = _sessionPool->getSession(opCtx);
    }
}

void MooseRecoveryUnit::_txnOpen(OperationContext* opCtx) {
    invariant(!_active);
    _ensureSession(opCtx);

    SqliteStatement::execQuery(_session.get(), "BEGIN");

    _active = true;
}

void MooseRecoveryUnit::_txnClose(bool commit) {
    invariant(_active);

    if (commit) {
        SqliteStatement::execQuery(_session.get(), "COMMIT");
    } else {
        SqliteStatement::execQuery(_session.get(), "ROLLBACK");
    }

    _active = false;
}
}
