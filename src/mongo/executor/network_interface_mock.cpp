/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */


#include <absl/container/node_hash_map.h>
#include <absl/meta/type_traits.h>
#include <boost/move/utility_core.hpp>
#include <boost/optional/optional.hpp>
// IWYU pragma: no_include "cxxabi.h"
// IWYU pragma: no_include "ext/alloc_traits.h"
#include <algorithm>
#include <functional>
#include <iterator>
#include <type_traits>

#include "mongo/base/error_codes.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/executor/network_connection_hook.h"
#include "mongo/executor/network_interface_mock.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_attr.h"
#include "mongo/logv2/log_component.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "mongo/util/time_support.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kNetwork


namespace mongo {
namespace executor {

namespace {
const Status kNetworkInterfaceMockShutdownInProgress = {
    ErrorCodes::ShutdownInProgress, "NetworkInterfaceMock shutdown in progress"};
}

using CallbackHandle = TaskExecutor::CallbackHandle;
using ResponseStatus = TaskExecutor::ResponseStatus;

NetworkInterfaceMock::NetworkInterfaceMock()
    : _clkSource(std::make_unique<ClockSourceMock>()),
      _waitingToRunMask(0),
      _currentlyRunning(kNoThread),
      _hasStarted(false),
      _inShutdown(false),
      _executorNextWakeupDate(Date_t::max()) {}

NetworkInterfaceMock::~NetworkInterfaceMock() {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    invariant(!_hasStarted || inShutdown());
}

std::string NetworkInterfaceMock::getDiagnosticString() {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    return str::stream() << "NetworkInterfaceMock -- waitingToRunMask:" << _waitingToRunMask
                         << ", now:" << _now_inlock().toString() << ", hasStarted:" << _hasStarted
                         << ", inShutdown: " << _inShutdown.load()
                         << ", operations: " << _operations.size()
                         << ", responses: " << _responses.size();
}

Date_t NetworkInterfaceMock::now() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    return _now_inlock();
}

std::string NetworkInterfaceMock::getHostName() {
    return "thisisourhostname";
}

/**
 * Starts a remote command with an implementation common to both the exhaust and non-exhaust
 * variants.
 */
SemiFuture<TaskExecutor::ResponseStatus> NetworkInterfaceMock::_startCommand(
    const TaskExecutor::CallbackHandle& cbHandle,
    RemoteCommandRequest& request,
    const BatonHandle& baton) {
    if (inShutdown()) {
        uassertStatusOK(kNetworkInterfaceMockShutdownInProgress);
    }

    stdx::lock_guard<stdx::mutex> lk(_mutex);

    const Date_t now = _now_inlock();

    LOGV2(5440600, "Scheduling request", "when"_attr = now, "request"_attr = request);

    auto [promise, future] = makePromiseFuture<TaskExecutor::ResponseStatus>();
    auto op = NetworkOperation(cbHandle, request, now, std::move(promise));

    // If we don't have a hook, or we have already 'connected' to this host, enqueue the op.
    if (!_hook || _connections.count(request.target)) {
        _enqueueOperation_inlock(std::move(op));
    } else {
        _connectThenEnqueueOperation_inlock(request.target, std::move(op));
    }

    return std::move(future).semi();
}

SemiFuture<TaskExecutor::ResponseStatus> NetworkInterfaceMock::startCommand(
    const CallbackHandle& cbHandle, RemoteCommandRequest& request, const BatonHandle& baton) {
    return _startCommand(cbHandle, request, baton);
}

Status NetworkInterfaceMock::startExhaustCommand(const CallbackHandle& cbHandle,
                                                 RemoteCommandRequest& request,
                                                 RemoteCommandOnReplyFn&& onReply,
                                                 const BatonHandle& baton) {
    // TODO: SERVER-93114 Call into _startCommand()
    MONGO_UNREACHABLE;
}

void NetworkInterfaceMock::setHandshakeReplyForHost(
    const mongo::HostAndPort& host, mongo::executor::RemoteCommandResponse&& reply) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    auto it = _handshakeReplies.find(host);
    if (it == std::end(_handshakeReplies)) {
        auto res = _handshakeReplies.emplace(host, std::move(reply));
        invariant(res.second);
    } else {
        it->second = std::move(reply);
    }
}

void NetworkInterfaceMock::cancelCommand(const CallbackHandle& cbHandle, const BatonHandle& baton) {
    invariant(!inShutdown());

    stdx::lock_guard<stdx::mutex> lk(_mutex);
    ResponseStatus rs(ErrorCodes::CallbackCanceled, "Network operation canceled", Milliseconds(0));

    _interruptWithResponse_inlock(cbHandle, rs);
}

void NetworkInterfaceMock::_interruptWithResponse_inlock(const CallbackHandle& cbHandle,
                                                         const ResponseStatus& response) {

    auto matchFn = [&cbHandle](const auto& ops) {
        return ops.isForCallback(cbHandle);
    };
    auto noi = std::find_if(_operations.begin(), _operations.end(), matchFn);

    // We've effectively observed the NetworkOperation.
    noi->markAsProcessing();
    _scheduleResponse_inlock(noi, _now_inlock(), response);
}

SemiFuture<void> NetworkInterfaceMock::setAlarm(const Date_t when, const CancellationToken& token) {
    if (inShutdown()) {
        return kNetworkInterfaceMockShutdownInProgress;
    }

    stdx::unique_lock<stdx::mutex> lk(_mutex);

    if (when <= _now_inlock()) {
        return Status::OK();
    }

    auto [promise, future] = makePromiseFuture<void>();
    auto id = _nextAlarmId++;

    token.onCancel().unsafeToInlineFuture().getAsync([this, id](Status status) {
        if (status.isOK()) {
            // Alarms live in a priority queue, so removing them isn't worth it
            // Thus we add the handle to a map and check at fire time
            stdx::unique_lock<stdx::mutex> lk(_mutex);
            _canceledAlarms.insert(id);
        }
    });

    _alarms.emplace(id, when, std::move(promise));

    return std::move(future).semi();
}

Status NetworkInterfaceMock::schedule(unique_function<void(Status)> action) {
    // Call the task immediately, we have no out-of-line executor
    action(Status::OK());

    // Say we scheduled the task fine, because we ran it inline
    return Status::OK();
}

bool NetworkInterfaceMock::onNetworkThread() {
    return _currentlyRunning == kNetworkThread;
}

void NetworkInterfaceMock::startup() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _startup_inlock();
}

void NetworkInterfaceMock::_startup_inlock() {
    invariant(!_hasStarted);
    _hasStarted = true;
    _inShutdown.store(false);
    invariant(_currentlyRunning == kNoThread);
    _currentlyRunning = kExecutorThread;
}

void NetworkInterfaceMock::shutdown() {
    invariant(!inShutdown());

    stdx::unique_lock<stdx::mutex> lk(_mutex);
    if (!_hasStarted) {
        _startup_inlock();
    }
    _inShutdown.store(true);
    auto todo = std::exchange(_operations, {});

    const Date_t now = _now_inlock();
    _waitingToRunMask |= kExecutorThread;  // Prevents network thread from scheduling.
    lk.unlock();
    for (auto& op : todo) {
        auto response = NetworkResponse{{},
                                        now,
                                        ResponseStatus{ErrorCodes::ShutdownInProgress,
                                                       "Shutting down mock network",
                                                       Milliseconds(0)}};
        if (op.fulfillResponse(std::move(response))) {
            LOGV2_WARNING(22590,
                          "Mock network interface shutting down with outstanding request",
                          "request"_attr = op.getRequest());
        }
    }
    lk.lock();
    invariant(_currentlyRunning == kExecutorThread);
    _currentlyRunning = kNoThread;
    _waitingToRunMask = kNetworkThread;
    _shouldWakeNetworkCondition.notify_one();
}

bool NetworkInterfaceMock::inShutdown() const {
    return _inShutdown.load();
}

void NetworkInterfaceMock::enterNetwork() {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    while (!_isNetworkThreadRunnable_inlock()) {
        _shouldWakeNetworkCondition.wait(lk);
    }
    _currentlyRunning = kNetworkThread;
    _waitingToRunMask &= ~kNetworkThread;
}

void NetworkInterfaceMock::exitNetwork() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    if (_currentlyRunning != kNetworkThread) {
        return;
    }
    _currentlyRunning = kNoThread;
    if (_isExecutorThreadRunnable_inlock()) {
        _shouldWakeExecutorCondition.notify_one();
    }
    _waitingToRunMask |= kNetworkThread;
}

bool NetworkInterfaceMock::hasReadyRequests() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);
    return _hasReadyRequests_inlock();
}

bool NetworkInterfaceMock::_hasReadyRequests_inlock() {
    auto noi = std::find_if(
        _operations.begin(), _operations.end(), [](auto& op) { return op.hasReadyRequest(); });
    return noi != _operations.end();
}

bool NetworkInterfaceMock::isNetworkOperationIteratorAtEnd(
    const NetworkInterfaceMock::NetworkOperationIterator& itr) {
    return itr == _operations.end();
}

NetworkInterfaceMock::NetworkOperationIterator NetworkInterfaceMock::getNextReadyRequest() {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);

    auto findNextReadyRequest = [&] {
        return std::find_if(
            _operations.begin(), _operations.end(), [](auto& op) { return op.hasReadyRequest(); });
    };

    auto noi = findNextReadyRequest();
    while (noi == _operations.end()) {
        _waitingToRunMask |= kExecutorThread;
        _runReadyNetworkOperations_inlock(&lk);

        noi = findNextReadyRequest();
    }
    noi->markAsProcessing();

    return noi;
}

NetworkInterfaceMock::NetworkOperationIterator NetworkInterfaceMock::getFrontOfUnscheduledQueue() {
    return getNthUnscheduledRequest(0);
}

NetworkInterfaceMock::NetworkOperationIterator NetworkInterfaceMock::getNthUnscheduledRequest(
    size_t n) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);

    // Linear time, but it's just for testing so no big deal.
    auto noi = _operations.begin();
    for (; noi != _operations.end(); ++noi) {
        if (noi->hasReadyRequest()) {
            if (n == 0) {
                return noi;
            } else {
                --n;
            }
        }
    }

    return _operations.end();
}

void NetworkInterfaceMock::_scheduleResponse_inlock(NetworkOperationIterator noi,
                                                    Date_t when,
                                                    const TaskExecutor::ResponseStatus& response) {
    auto insertBefore = std::find_if(_responses.begin(),
                                     _responses.end(),
                                     [when](const auto& response) { return response.when > when; });

    _responses.insert(insertBefore, NetworkResponse{noi, when, response});
    LOGV2(5440601,
          "Scheduling response",
          "when"_attr = when,
          "request"_attr = noi->getRequest(),
          "response"_attr = response);
}

void NetworkInterfaceMock::scheduleResponse(NetworkOperationIterator noi,
                                            Date_t when,
                                            const TaskExecutor::ResponseStatus& response) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);
    noi->assertNotBlackholed();
    _scheduleResponse_inlock(noi, when, response);
}

RemoteCommandRequest NetworkInterfaceMock::scheduleSuccessfulResponse(const BSONObj& response) {
    return scheduleSuccessfulResponse(RemoteCommandResponse(response, Milliseconds(0)));
}

RemoteCommandRequest NetworkInterfaceMock::scheduleSuccessfulResponse(
    const RemoteCommandResponse& response) {
    return scheduleSuccessfulResponse(getNextReadyRequest(), response);
}

RemoteCommandRequest NetworkInterfaceMock::scheduleSuccessfulResponse(
    NetworkOperationIterator noi, const RemoteCommandResponse& response) {
    return scheduleSuccessfulResponse(noi, now(), response);
}

RemoteCommandRequest NetworkInterfaceMock::scheduleSuccessfulResponse(
    NetworkOperationIterator noi, Date_t when, const RemoteCommandResponse& response) {
    scheduleResponse(noi, when, response);
    return noi->getRequest();
}

RemoteCommandRequest NetworkInterfaceMock::scheduleErrorResponse(const Status& response) {
    return scheduleErrorResponse(getNextReadyRequest(), response);
}

RemoteCommandRequest NetworkInterfaceMock::scheduleErrorResponse(const ResponseStatus response) {
    auto noi = getNextReadyRequest();
    scheduleResponse(noi, now(), response);
    return noi->getRequest();
}

RemoteCommandRequest NetworkInterfaceMock::scheduleErrorResponse(NetworkOperationIterator noi,
                                                                 const Status& response) {
    return scheduleErrorResponse(noi, now(), response);
}

RemoteCommandRequest NetworkInterfaceMock::scheduleErrorResponse(NetworkOperationIterator noi,
                                                                 Date_t when,
                                                                 const Status& response) {
    scheduleResponse(noi, when, response);
    return noi->getRequest();
}

void NetworkInterfaceMock::blackHole(NetworkOperationIterator noi) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);
    noi->markAsBlackholed();
}

Date_t NetworkInterfaceMock::runUntil(Date_t until) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);
    invariant(until > _now_inlock());
    while (until > _now_inlock()) {
        _runReadyNetworkOperations_inlock(&lk);
        if (_hasReadyRequests_inlock()) {
            break;
        }
        Date_t newNow = _executorNextWakeupDate;
        if (!_alarms.empty() && _alarms.top().when < newNow) {
            newNow = _alarms.top().when;
        }
        if (!_responses.empty() && _responses.front().when < newNow) {
            newNow = _responses.front().when;
        }
        if (until < newNow) {
            newNow = until;
        }

        auto duration = newNow - _now_inlock();
        invariant(duration >= Milliseconds{0});
        _clkSource->advance(duration);

        _waitingToRunMask |= kExecutorThread;
    }
    _runReadyNetworkOperations_inlock(&lk);
    return _now_inlock();
}

void NetworkInterfaceMock::advanceTime(Date_t newTime) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);

    auto duration = newTime - _now_inlock();
    invariant(duration > Milliseconds{0});
    _clkSource->advance(duration);

    _waitingToRunMask |= kExecutorThread;
    _runReadyNetworkOperations_inlock(&lk);
}

void NetworkInterfaceMock::runReadyNetworkOperations() {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);
    _runReadyNetworkOperations_inlock(&lk);
}

void NetworkInterfaceMock::waitForWork() {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kExecutorThread);
    _waitForWork_inlock(&lk);
}

void NetworkInterfaceMock::waitForWorkUntil(Date_t when) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kExecutorThread);
    _executorNextWakeupDate = when;
    if (_executorNextWakeupDate <= _now_inlock()) {
        return;
    }
    _waitForWork_inlock(&lk);
}

void NetworkInterfaceMock::_enqueueOperation_inlock(NetworkOperation&& op) {
    const auto timeout = op.getRequest().timeout;
    auto cbh = op.getCallbackHandle();
    _operations.emplace_back(std::forward<NetworkOperation>(op));

    if (timeout != RemoteCommandRequest::kNoTimeout) {
        invariant(timeout >= Milliseconds(0));
        auto [promise, future] = makePromiseFuture<void>();
        _alarms.emplace(_nextAlarmId++, _now_inlock() + timeout, std::move(promise));
        std::move(future).getAsync([this, cbh](Status status) {
            if (!status.isOK()) {
                return;
            }
            auto response = ResponseStatus(
                ErrorCodes::NetworkInterfaceExceededTimeLimit, "Network timeout", Milliseconds(0));
            _interruptWithResponse_inlock(cbh, std::move(response));
        });
    }
}

void NetworkInterfaceMock::_connectThenEnqueueOperation_inlock(const HostAndPort& target,
                                                               NetworkOperation&& op) {
    invariant(_hook);  // if there is no hook, we shouldn't even hit this codepath
    invariant(!_connections.count(target));

    auto handshakeReplyIter = _handshakeReplies.find(target);

    auto handshakeReply = (handshakeReplyIter != std::end(_handshakeReplies))
        ? handshakeReplyIter->second
        : RemoteCommandResponse(BSONObj(), Milliseconds(0));

    auto valid = _hook->validateHost(target, op.getRequest().cmdObj, handshakeReply);
    if (!valid.isOK()) {
        auto response = NetworkResponse{{}, _now_inlock(), valid};
        op.fulfillResponse(std::move(response));
        return;
    }

    auto swHookPostconnectCommand = _hook->makeRequest(target);

    if (!swHookPostconnectCommand.isOK()) {
        auto response = NetworkResponse{{}, _now_inlock(), swHookPostconnectCommand.getStatus()};
        op.fulfillResponse(std::move(response));
        return;
    }

    boost::optional<RemoteCommandRequest> hookPostconnectCommand =
        std::move(swHookPostconnectCommand.getValue());

    if (!hookPostconnectCommand) {
        // If we don't have a post connect command, enqueue the actual command.
        _connections.emplace(op.getRequest().target);
        _enqueueOperation_inlock(std::move(op));
        return;
    }

    auto cbh = op.getCallbackHandle();

    auto [promise, future] = makePromiseFuture<TaskExecutor::ResponseStatus>();
    std::move(future).getAsync(
        [this, op = std::move(op)](StatusWith<TaskExecutor::ResponseStatus> swRs) mutable {
            if (!swRs.isOK()) {
                return;
            }

            auto rs = swRs.getValue();
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            if (!rs.isOK()) {
                auto response = NetworkResponse{{}, _now_inlock(), rs};
                op.fulfillResponse(std::move(response));
                return;
            }

            auto handleStatus = _hook->handleReply(op.getRequest().target, std::move(rs));
            if (!handleStatus.isOK()) {
                auto response = NetworkResponse{{}, _now_inlock(), handleStatus};
                op.fulfillResponse(std::move(response));
                return;
            }

            _connections.emplace(op.getRequest().target);
            _enqueueOperation_inlock(std::move(op));
        });
    auto postconnectOp = NetworkOperation(
        cbh, std::move(*hookPostconnectCommand), _now_inlock(), std::move(promise));

    _enqueueOperation_inlock(std::move(postconnectOp));
}

void NetworkInterfaceMock::setConnectionHook(std::unique_ptr<NetworkConnectionHook> hook) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(!_hasStarted);
    invariant(!_hook);
    _hook = std::move(hook);
}

void NetworkInterfaceMock::setEgressMetadataHook(
    std::unique_ptr<rpc::EgressMetadataHook> metadataHook) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(!_hasStarted);
    invariant(!_metadataHook);
    _metadataHook = std::move(metadataHook);
}

void NetworkInterfaceMock::signalWorkAvailable() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _waitingToRunMask |= kExecutorThread;
    if (_currentlyRunning == kNoThread) {
        _shouldWakeExecutorCondition.notify_one();
    }
}

void NetworkInterfaceMock::_runReadyNetworkOperations_inlock(stdx::unique_lock<stdx::mutex>* lk) {
    while (!_alarms.empty() && _now_inlock() >= _alarms.top().when) {
        // It's safe to remove the const qualifier here as we immediately remove the top.
        AlarmInfo alarm = std::move(const_cast<AlarmInfo&>(_alarms.top()));
        _alarms.pop();

        // If the handle isn't cancelled, then run it
        auto iter = _canceledAlarms.find(alarm.id);
        if (iter == _canceledAlarms.end()) {
            lk->unlock();
            alarm.promise.emplaceValue();
            lk->lock();
        } else {
            _canceledAlarms.erase(iter);
            lk->unlock();
            alarm.promise.setError({ErrorCodes::CallbackCanceled, "Alarm cancelled"});
            lk->lock();
        }
    }
    while (!_responses.empty() && _now_inlock() >= _responses.front().when) {
        invariant(_currentlyRunning == kNetworkThread);
        auto response = std::exchange(_responses.front(), {});
        _responses.pop_front();
        _waitingToRunMask |= kExecutorThread;
        lk->unlock();

        auto noi = response.noi;

        LOGV2(5440602,
              "Processing response",
              "when"_attr = response.when,
              "request"_attr = noi->getRequest(),
              "response"_attr = response.response);

        if (_metadataHook) {
            _metadataHook->readReplyMetadata(noi->getRequest().opCtx, response.response.data)
                .transitional_ignore();
        }

        // The NetworkInterface can recieve multiple responses for a particular request (e.g.
        // cancellation and a 'true' scheduled response). But each request can only have one logical
        // response. This choice of the one logical response is mediated by the _isFinished field of
        // the NetworkOperation; whichever response sets this first via
        // NetworkOperation::fulfillResponse wins. NetworkOperation::fulfillResponse returns `true`
        // if the given response was accepted by the NetworkOperation as its sole logical response.
        //
        // We care about this here because we only want to increment the counters for operations
        // succeeded/failed for the responses that are actually used,
        Status localResponseStatus = response.response.status;
        bool noiUsedThisResponse = noi->fulfillResponse(std::move(response));
        if (noiUsedThisResponse) {
            _counters.sent++;
            if (localResponseStatus.isOK()) {
                _counters.succeeded++;
            } else if (ErrorCodes::isCancellationError(localResponseStatus)) {
                _counters.canceled++;
            } else {
                _counters.failed++;
            }
        }
        lk->lock();
    }
    invariant(_currentlyRunning == kNetworkThread);
    if (!(_waitingToRunMask & kExecutorThread)) {
        return;
    }
    _shouldWakeExecutorCondition.notify_one();
    _currentlyRunning = kNoThread;
    while (!_isNetworkThreadRunnable_inlock()) {
        _shouldWakeNetworkCondition.wait(*lk);
    }
    _currentlyRunning = kNetworkThread;
    _waitingToRunMask &= ~kNetworkThread;
}

bool NetworkInterfaceMock::hasReadyNetworkOperations() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_currentlyRunning == kNetworkThread);
    if (!_alarms.empty() && _now_inlock() >= _alarms.top().when) {
        return true;
    }

    if (!_responses.empty() && _responses.front().when <= _now_inlock()) {
        return true;
    }
    return false;
}

void NetworkInterfaceMock::_waitForWork_inlock(stdx::unique_lock<stdx::mutex>* lk) {
    if (_waitingToRunMask & kExecutorThread) {
        _waitingToRunMask &= ~kExecutorThread;
        return;
    }
    _currentlyRunning = kNoThread;
    while (!_isExecutorThreadRunnable_inlock()) {
        _waitingToRunMask |= kNetworkThread;
        _shouldWakeNetworkCondition.notify_one();
        _shouldWakeExecutorCondition.wait(*lk);
    }
    _currentlyRunning = kExecutorThread;
    _waitingToRunMask &= ~kExecutorThread;
}

bool NetworkInterfaceMock::_isNetworkThreadRunnable_inlock() {
    if (_currentlyRunning != kNoThread) {
        return false;
    }
    if (_waitingToRunMask != kNetworkThread) {
        return false;
    }
    return true;
}

bool NetworkInterfaceMock::_isExecutorThreadRunnable_inlock() {
    if (_currentlyRunning != kNoThread) {
        return false;
    }
    return _waitingToRunMask & kExecutorThread;
}

NetworkInterfaceMock::NetworkOperation::NetworkOperation()
    : _requestDate(), _request(), _respPromise() {}

NetworkInterfaceMock::NetworkOperation::NetworkOperation(
    const CallbackHandle& cbHandle,
    const RemoteCommandRequest& theRequest,
    Date_t theRequestDate,
    Promise<TaskExecutor::ResponseStatus> promise)
    : _requestDate(theRequestDate),
      _cbHandle(cbHandle),
      _request(theRequest),
      _respPromise(std::move(promise)) {}

std::string NetworkInterfaceMock::NetworkOperation::getDiagnosticString() const {
    return str::stream() << "NetworkOperation -- request:'" << _request.toString()
                         << ", reqDate: " << _requestDate.toString();
}

bool NetworkInterfaceMock::NetworkOperation::fulfillResponse(NetworkResponse response) {
    if (_isFinished) {
        // Nothing to do.
        return false;
    }

    // If there's no more to come, then we're done after this response.
    _isFinished = !response.response.moreToCome;

    response.response.target = _request.target;
    _respPromise.emplaceValue(response.response);

    return true;
}

NetworkInterfaceMock::InNetworkGuard::InNetworkGuard(NetworkInterfaceMock* net) : _net(net) {
    _net->enterNetwork();
}

void NetworkInterfaceMock::InNetworkGuard::dismiss() {
    _callExitNetwork = false;
    _net->exitNetwork();
}

NetworkInterfaceMock::InNetworkGuard::~InNetworkGuard() {
    if (_callExitNetwork)
        _net->exitNetwork();
}

NetworkInterfaceMock* NetworkInterfaceMock::InNetworkGuard::operator->() const {
    return _net;
}

}  // namespace executor
}  // namespace mongo
