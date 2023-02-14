/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "pinned_connection_task_executor.h"

namespace mongo::executor {

PinnedConnectionTaskExecutor::PinnedConnectionTaskExecutor(
    const std::shared_ptr<TaskExecutor>& executor)
    : _underlyingExecutor(executor) {}

Date_t PinnedConnectionTaskExecutor::now() {
    return _executor()->now();
}

StatusWith<TaskExecutor::EventHandle> PinnedConnectionTaskExecutor::makeEvent() {
    return _executor()->makeEvent();
}

void PinnedConnectionTaskExecutor::signalEvent(const EventHandle& event) {
    return _executor()->signalEvent(event);
}

StatusWith<TaskExecutor::CallbackHandle> PinnedConnectionTaskExecutor::onEvent(
    const EventHandle& event, CallbackFn&& work) {
    return _executor()->onEvent(event, std::move(work));
}

void PinnedConnectionTaskExecutor::waitForEvent(const EventHandle& event) {
    _executor()->waitForEvent(event);
}

StatusWith<stdx::cv_status> PinnedConnectionTaskExecutor::waitForEvent(OperationContext* opCtx,
                                                                       const EventHandle& event,
                                                                       Date_t deadline) {
    return _executor()->waitForEvent(opCtx, event, deadline);
}

StatusWith<TaskExecutor::CallbackHandle> PinnedConnectionTaskExecutor::scheduleWork(
    CallbackFn&& work) {
    return _executor()->scheduleWork(std::move(work));
}

StatusWith<TaskExecutor::CallbackHandle> PinnedConnectionTaskExecutor::scheduleWorkAt(
    Date_t when, CallbackFn&& work) {
    return _executor()->scheduleWorkAt(when, std::move(work));
}

StatusWith<TaskExecutor::CallbackHandle> PinnedConnectionTaskExecutor::scheduleRemoteCommandOnAny(
    const RemoteCommandRequestOnAny& requestOnAny,
    const RemoteCommandOnAnyCallbackFn& cb,
    const BatonHandle& baton) {
    // TODO: SERVER-73611. Implement RPC functionality over a single network connection for the
    // entire PinnedConnectionTaskExecutor's lifetime.
    MONGO_UNIMPLEMENTED;
}

void PinnedConnectionTaskExecutor::cancel(const CallbackHandle& cbHandle) {
    // TODO: SERVER-73611. Implement cancellation for RPCs
    _executor()->cancel(cbHandle);
}

void PinnedConnectionTaskExecutor::wait(const CallbackHandle& cbHandle,
                                        Interruptible* interruptible) {
    // TODO: SERVER-73611. Either implement waiting for RPCs, or document that it is banned.
    _executor()->wait(cbHandle, interruptible);
}

// Below are the portions of the TaskExecutor API that are illegal to use through
// PinnedCursorTaskExecutor and/or are unimplemented at this time.
StatusWith<TaskExecutor::CallbackHandle>
PinnedConnectionTaskExecutor::scheduleExhaustRemoteCommandOnAny(
    const RemoteCommandRequestOnAny& request,
    const RemoteCommandOnAnyCallbackFn& cb,
    const BatonHandle& baton) {
    MONGO_UNIMPLEMENTED;
}

bool PinnedConnectionTaskExecutor::hasTasks() {
    // TODO: SERVER-73611. Consider tracking RPC tasks here and unbanning from API,
    // with a clear documented contract.
    MONGO_UNIMPLEMENTED;
}

void PinnedConnectionTaskExecutor::startup() {
    MONGO_UNIMPLEMENTED;
}

void PinnedConnectionTaskExecutor::shutdown() {
    MONGO_UNIMPLEMENTED;
}

void PinnedConnectionTaskExecutor::join() {
    MONGO_UNIMPLEMENTED;
}

SharedSemiFuture<void> PinnedConnectionTaskExecutor::joinAsync() {
    MONGO_UNIMPLEMENTED;
}

bool PinnedConnectionTaskExecutor::isShuttingDown() const {
    MONGO_UNIMPLEMENTED;
}

void PinnedConnectionTaskExecutor::appendDiagnosticBSON(mongo::BSONObjBuilder* builder) const {
    MONGO_UNIMPLEMENTED;
}


void PinnedConnectionTaskExecutor::appendConnectionStats(ConnectionPoolStats* stats) const {
    MONGO_UNIMPLEMENTED;
}

void PinnedConnectionTaskExecutor::dropConnections(const HostAndPort& hostAndPort) {
    MONGO_UNIMPLEMENTED;
}

void PinnedConnectionTaskExecutor::appendNetworkInterfaceStats(BSONObjBuilder& bob) const {
    MONGO_UNIMPLEMENTED;
}

}  // namespace mongo::executor
