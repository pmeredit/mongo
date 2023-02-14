/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */
#pragma once

#include <memory>

#include "mongo/executor/task_executor.h"

namespace mongo::executor {

/**
 * Implementation of a TaskExecutor that provides the ability to schedule RPC/networking on the same
 * underlying network connection. The PinnedTaskExecutor is constructed from another TaskExecutor,
 * and uses that TaskExecutor's ThreadPool and NetworkInterface/networking reactor to perform work.
 * Specifically:
 * - Functions that schedule work or manage events that happen locally, without going over the
 *   network, are passed-through directly to the underlying TaskExecutor (i.e. scheduleWork,
 *   makeEvent, waitForEvent).
 * - Functions that involve scheduling RPC/networking are all run on the same underlying
 *   network-connection (i.e. TCP/Unix Domain Socket).
 * Note that this means that the PinnedConnectionTaskExecutor can only speak to one host over its
 * entire lifetime! If you need to speak to a different host, you need a different connection, so
 * construct a *new* PinnedCursorTaskExecutor from the underlying executor.
 *
 * Certain methods are illegal to call. The lifecycle-management functions, including:
 * - startup()
 * - shutdown()
 * - join()
 * are illegal to call because this TaskExecutor's lifecylce is tied to another and provides
 * no additional lifetime support. Ensure the underlying TaskExecutor is alive for the
 * lifetime of this object instead. Additionally, diagnostic and network management methods:
 * - appendDiagnosticBSON()
 * - appendConnectionStats()
 * - dropConnections()
 * - appendNetworkInterfaceStats()
 * - hasTasks()
 * are illegal to call because this TaskExecutor provides a distinct networking API. Gather
 * diagnostics from the underlying TaskExecutor instead if needed.
 *
 * Exhaust commands are not supported at this time.
 */
class PinnedConnectionTaskExecutor final : public TaskExecutor {
    PinnedConnectionTaskExecutor(const PinnedConnectionTaskExecutor&) = delete;
    PinnedConnectionTaskExecutor& operator=(const PinnedConnectionTaskExecutor&) = delete;

public:
    PinnedConnectionTaskExecutor(const std::shared_ptr<TaskExecutor>& executor);

    // Lifetime management portion of the TaskExecutor API. Since this type is a view over
    // an underlying TaskExecutor, these are illegal to call on this view-type.
    void startup() override;
    void shutdown() override;
    void join() override;
    SharedSemiFuture<void> joinAsync() override;
    bool isShuttingDown() const override;

    // These pass-through to the underlying TaskExecutor.
    Date_t now() override;
    StatusWith<EventHandle> makeEvent() override;
    void signalEvent(const EventHandle& event) override;
    StatusWith<CallbackHandle> onEvent(const EventHandle& event, CallbackFn&& work) override;
    void waitForEvent(const EventHandle& event) override;
    StatusWith<stdx::cv_status> waitForEvent(OperationContext* opCtx,
                                             const EventHandle& event,
                                             Date_t deadline) override;
    StatusWith<CallbackHandle> scheduleWork(CallbackFn&& work) override;
    StatusWith<CallbackHandle> scheduleWorkAt(Date_t when, CallbackFn&& work) override;

    // This type provides special connection-pinning behavior for RPC functionality here.
    StatusWith<CallbackHandle> scheduleRemoteCommandOnAny(
        const RemoteCommandRequestOnAny& request,
        const RemoteCommandOnAnyCallbackFn& cb,
        const BatonHandle& baton = nullptr) override;

    StatusWith<CallbackHandle> scheduleExhaustRemoteCommandOnAny(
        const RemoteCommandRequestOnAny& request,
        const RemoteCommandOnAnyCallbackFn& cb,
        const BatonHandle& baton = nullptr) override;

    // When cancel() is passed a CallbackHandle that was returned from schedule{Work}()/onEvent(),
    // cancellation is passed-through to the underlying executor. If the CallbackHandle was returned
    // from scheduleRemoteCommand then the executor will cancel the RPC attempt. Users are
    // responsible for cancelling any events they schedule through this object. It will clean up any
    // pending RPC operations, but will not cancel all outstanding non-RPC work.
    void cancel(const CallbackHandle& cbHandle) override;
    void wait(const CallbackHandle& cbHandle,
              Interruptible* interruptible = Interruptible::notInterruptible()) override;

    // Illegal to call because the view does not track it's portion of the underlying TaskExecutor's
    // resources.
    void appendConnectionStats(ConnectionPoolStats*) const override;
    void appendNetworkInterfaceStats(BSONObjBuilder&) const override;
    void appendDiagnosticBSON(BSONObjBuilder*) const override;
    void dropConnections(const HostAndPort&) override;
    bool hasTasks() override;

private:
    // Get a safe-to-use pointer to our underlying executor.
    std::shared_ptr<TaskExecutor> _executor() {
        auto exec = _underlyingExecutor.lock();
        invariant(exec, "Parent executor must outlive sub-executor");
        return exec;
    }

    std::weak_ptr<TaskExecutor> _underlyingExecutor;
};

}  // namespace mongo::executor
