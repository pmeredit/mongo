/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/assert_util_core.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/scopeguard.h"
#include <boost/optional/optional.hpp>
#include <deque>
#include <functional>

namespace streams {

/**
 * This class implements a bounded, blocking, thread-safe Producer-Consumer queue.
 *
 * Properties:
 *   bounded - the queue can be limited to a number of items and the total bytes
 *   blocking - when the queue is full, the producer blocks and when the queue is empty, the
 *              consumer blocks.
 *   thread-safe - the queue can be accessed safely from multiple threads at the same time
 *
 * Cost Function:
 *   The cost function is provided by the owner of this queue and must return Cost that contains the
 *   number of items and the total bytes. Cost function is used to set the bounds on the queue size.
 */
template <typename T>
class ProducerConsumerQueue {
public:
    ~ProducerConsumerQueue() {
        using mongo::kDebugBuild;
        dassert(_shutDown);
    }

    /*
     * The cost of an entry to be added to the queue.
     * Consumer provides a cost function that returns the Cost of each entry,
     * which includes the count and size in bytes.
     */
    struct Cost {
        // Total count in the entry/message.
        int64_t entrySize{0};

        // Total bytes in the entry/message.
        int64_t entrySizeBytes{0};

        bool operator==(const Cost& cost) const {
            return cost.entrySize == entrySize && cost.entrySizeBytes == entrySizeBytes;
        }
    };

    // Cost function is provided by the consumer of the queue, and is used to calculate if there is
    // enough space in the queue to insert the new entry.
    using CostFunc = std::function<Cost(const T&)>;

    struct Options {
        CostFunc costFunc;
        int64_t maxQueueSize = std::numeric_limits<int64_t>::max();
        int64_t maxQueueSizeBytes = std::numeric_limits<int64_t>::max();
    };

    ProducerConsumerQueue(Options options) : _options(std::move(options)) {}

    // Push the passed T into the queue.
    // Blocks if, as per the cost function, there is not enough space in the queue.
    mongo::Status push(T t) {
        mongo::stdx::unique_lock<mongo::stdx::mutex> lk(_mutex);
        Cost cost = _options.costFunc(t);

        if (!waitForSpace(lk)) {
            return {mongo::ErrorCodes::InternalError, "Producer Consumer queue is shut down."};
        }

        pushInner(lk, std::move(t), cost);
        _consumerCv.notify_all();
        return mongo::Status::OK();
    }

    // Pops one T out of the queue.
    // Blocks if the queue is empty and return none if shut down.
    boost::optional<T> pop() {
        mongo::stdx::unique_lock<mongo::stdx::mutex> lk(_mutex);

        if (waitForNonEmpty(lk)) {
            const mongo::ScopeGuard guard([&] { _producerCv.notify_all(); });
            return popInner(lk);
        }
        return boost::none;
    }

    // Wait for the queue to be empty.
    // Blocks if the queue is not empty.
    mongo::Status waitForEmpty() {
        mongo::stdx::unique_lock<mongo::stdx::mutex> lk(_mutex);

        if (!waitForEmpty(lk)) {
            return {mongo::ErrorCodes::InternalError, "Producer Consumer queue is shut down."};
        }
        return mongo::Status::OK();
    }

    // Check if the queue is empty.
    bool isEmpty() {
        mongo::stdx::unique_lock<mongo::stdx::mutex> lk(_mutex);
        return _queue.empty();
    }

    // Peeks the Cost of the next entry in the Queue. Returns none if the queue is empty or is shut
    // down.
    boost::optional<Cost> peekCost() {
        mongo::stdx::unique_lock<mongo::stdx::mutex> lk(_mutex);
        if (_shutDown || _queue.empty()) {
            return boost::none;
        }
        return _options.costFunc(_queue.front());
    }

    // Shuts down the queue.
    void shutDown() {
        mongo::stdx::unique_lock<mongo::stdx::mutex> lk(_mutex);
        if (!_shutDown) {
            _shutDown = true;
            _consumerCv.notify_all();
            _producerCv.notify_all();
        }
    }

private:
    friend class StreamsProducerConsumerQueueTest;

    void pushInner(mongo::WithLock, T t, Cost cost) {
        using mongo::kDebugBuild;
        dassert(!_shutDown);

        _queue.push_back(std::move(t));

        _currentSize += cost.entrySize;
        _currentSizeBytes += cost.entrySizeBytes;
    }

    T popInner(mongo::WithLock) {
        using mongo::kDebugBuild;
        dassert(!_shutDown && !_queue.empty());

        auto t = std::move(_queue.front());
        _queue.pop_front();

        auto cost = _options.costFunc(t);
        _currentSize -= cost.entrySize;
        _currentSizeBytes -= cost.entrySizeBytes;

        dassert(_currentSize >= 0 && _currentSizeBytes >= 0);
        return t;
    }

    bool waitForSpace(mongo::stdx::unique_lock<mongo::stdx::mutex>& lk) {
        auto canFit = [&]() -> bool {
            return (_currentSize < _options.maxQueueSize) &&
                (_currentSizeBytes < _options.maxQueueSizeBytes);
        };
        if (canFit()) {
            return true;
        }

        _producerCv.wait(lk, [&]() -> bool { return _shutDown || canFit(); });

        return !_shutDown;
    }

    bool waitForNonEmpty(mongo::stdx::unique_lock<mongo::stdx::mutex>& lk) {
        if (!_queue.empty()) {
            return true;
        }

        _consumerCv.wait(lk, [this]() { return _shutDown || !_queue.empty(); });

        return !_shutDown;
    }

    bool waitForEmpty(mongo::stdx::unique_lock<mongo::stdx::mutex>& lk) {
        if (_queue.empty()) {
            return true;
        }

        _producerCv.wait(lk, [this]() { return _shutDown || _queue.empty(); });

        return !_shutDown;
    }

    Options _options;

    mutable mongo::stdx::mutex _mutex;
    std::deque<T> _queue;
    int64_t _currentSize{0};
    int64_t _currentSizeBytes{0};
    bool _shutDown{false};
    mongo::stdx::condition_variable _consumerCv;
    mongo::stdx::condition_variable _producerCv;
};
}  // namespace streams
