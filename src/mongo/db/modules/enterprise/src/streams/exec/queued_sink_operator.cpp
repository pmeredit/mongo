/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/queued_sink_operator.h"

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include "mongo/base/error_codes.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/operator.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/util/exception.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;

QueuedSinkOperator::QueuedSinkOperator(Context* context, int32_t numInputs, int32_t parallelism)
    : SinkOperator(context, numInputs), _parallelism(parallelism) {
    _maxQueueSizeBytes = getMaxQueueSizeBytes(_context->featureFlags);
    if (_parallelism > 1) {
        _maxQueueSizeBytes = (_maxQueueSizeBytes / _parallelism) * 2;
    }
}

std::unique_ptr<QueuedSinkOperator::WriterThread> QueuedSinkOperator::makeThread(
    std::unique_ptr<SinkWriter> writer) {
    auto thread = std::make_unique<WriterThread>();
    thread->writer = std::move(writer);
    thread->context = _context;
    thread->queue =
        std::make_unique<mongo::SingleProducerSingleConsumerQueue<Message, QueueCostFunc>>(
            mongo::SingleProducerSingleConsumerQueue<Message, QueueCostFunc>::Options{
                .maxQueueDepth = static_cast<size_t>(_maxQueueSizeBytes)});
    return thread;
}

void QueuedSinkOperator::doStart() {
    tassert(ErrorCodes::InternalError,
            "parallelism should be greater than or equal to 1",
            _parallelism >= 1);

    // Create WriterThreads.
    for (int threadId = 0; threadId < _parallelism; ++threadId) {
        // SinkWriter instances are duplicates of one another.
        auto writer = makeWriter();
        _threads.push_back(makeThread(std::move(writer)));
    }

    // Start all the writer threads.
    for (size_t threadId = 0; threadId < _threads.size(); ++threadId) {
        _threads[threadId]->registerMetrics(_metricManager, threadId);
        _threads[threadId]->start();
    }
}

void QueuedSinkOperator::WriterThread::start() {
    tassert(ErrorCodes::InternalError,
            "Expected consumerThread to not already be started",
            !consumerThread.joinable());
    consumerThread = stdx::thread([this]() {
        // Validate the connection with the target.
        SPStatus status{Status::OK()};
        try {
            writer->connect();
        } catch (const SPException& e) {
            LOGV2_INFO(8520399,
                       "SPException occured in QueuedSinkOperator connect",
                       "context"_attr = context,
                       "code"_attr = e.code(),
                       "reason"_attr = e.reason(),
                       "unsafeErrorMessage"_attr = e.unsafeReason());
            status = e.toStatus();
        } catch (const DBException& e) {
            LOGV2_INFO(8520301,
                       "Exception occured in QueuedSinkOperator connect",
                       "context"_attr = context,
                       "code"_attr = e.code(),
                       "reason"_attr = e.reason());
            status = e.toStatus();
        } catch (const std::exception& e) {
            LOGV2_WARNING(8520300,
                          "Unexpected std::exception occured in QueuedSinkOperator connect",
                          "context"_attr = context,
                          "exception"_attr = e.what());
            status = SPStatus{{ErrorCodes::UnknownError, "Unkown error occured in sink operator."},
                              e.what()};
        }

        // If connect succeeded, enter a kConnected state.
        // Otherwise enter an error state and return.
        {
            stdx::lock_guard<stdx::mutex> lock(consumerMutex);
            if (status.isOK()) {
                consumerStatus = ConnectionStatus{ConnectionStatus::kConnected};
            } else {
                consumerStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
                consumerThreadRunning = false;
                flushedCv.notify_all();
                // Return early in error.
                return;
            }
        }

        // Start consuming messages.
        consumeLoop();
    });
    stdx::unique_lock<stdx::mutex> lock(consumerMutex);
    consumerThreadRunning = true;
}

void QueuedSinkOperator::doStop() {
    for (auto& thread : _threads) {
        thread->stop();
    }
}

void QueuedSinkOperator::WriterThread::stop() {
    // This will close the queue which will make the consumer thread exit as well
    // because this will trigger a `ProducerConsumerQueueConsumed` exception in the
    // consumer thread.
    queue->closeConsumerEnd();
    if (consumerThread.joinable()) {
        consumerThread.join();
    }
}

void QueuedSinkOperator::doFlush() {
    for (auto& thread : _threads) {
        thread->flush();
    }
}

void QueuedSinkOperator::WriterThread::flush() {
    stdx::unique_lock<stdx::mutex> lock(consumerMutex);

    dassert(!pendingFlush);
    pendingFlush = true;
    queue->push(Message{.flushSignal = true, .size = 1});
    flushedCv.wait(lock, [this]() -> bool { return !consumerThreadRunning || !pendingFlush; });

    // Make sure that an error wasn't encountered in the background consumer thread while
    // waiting for the flushed condvar to be notified.
    consumerStatus.throwIfNotConnected();
    uassert(75386, str::stream() << "Unable to flush queued sink operator", !pendingFlush);
}

void QueuedSinkOperator::registerMetrics(MetricManager* metricManager) {
    _metricManager = metricManager;
}

void QueuedSinkOperator::WriterThread::registerMetrics(MetricManager* metricManager, int threadID) {
    auto labels = getDefaultMetricLabels(context);
    labels.push_back(std::make_pair("sink_thread_id", std::to_string(threadID)));
    queueSizeGauge = metricManager->registerIntGauge(
        "sink_operator_queue_size",
        /* description */ "Total docs currently buffered in the queue",
        /*labels*/ labels);
    queueByteSizeGauge = metricManager->registerIntGauge(
        "sink_operator_queue_bytesize",
        /* description */ "Total bytes currently buffered in the queue",
        /*labels*/ labels);
    writer->registerMetrics(metricManager, std::move(labels));
}

OperatorStats QueuedSinkOperator::doGetStats() {
    _stats.connectionType = getConnectionType();
    for (auto& thread : _threads) {
        OperatorStats stats;
        {
            stdx::lock_guard<stdx::mutex> lock(thread->consumerMutex);
            std::swap(thread->consumerStats, stats);
        }
        incOperatorStats(stats);
    }
    return _stats;
}

void QueuedSinkOperator::doSinkOnDataMsg(int32_t inputIdx,
                                         StreamDataMsg dataMsg,
                                         boost::optional<StreamControlMsg> controlMsg) {
    tassert(ErrorCodes::InternalError,
            "Size of threads should be greater than or equal to 1",
            _threads.size());

    // Without this extra copy, some hopping window tests can fail with thread
    // sanitizer errors. This can also happen when using $unwind or other stages that
    // copy Documents. Why?
    // Often with a $hoppingWindow, a Document will be copied across multiple
    // windows. Under the hood this just increments a refcount to the same underlying storage.
    // So the following can happen:
    // The Executor thread closes window1 and sends its Documents to this QueuedSinkOperator.
    // Then the Executor thread closes window2, which happens to be operating on a copy
    // of some of window1's documents.
    // This might cause a race, the Executor thread calls DocumentStorage::currentApproximateSize()
    // while closing window2, while a WriterThread calls DocumentStorage::alloc(). Both these
    // threads are operating on copied Document instances which share their DocumentStorage.
    for (auto& doc : dataMsg.docs) {
        MutableDocument mut{doc.doc};
        mut.makeOwned();
        doc.doc = mut.freeze();
    }

    auto sendMsg = [this](WriterThread* thread, StreamDataMsg dataMsg) {
        auto size = dataMsg.getByteSize();
        thread->queueSizeGauge->incBy(int64_t(dataMsg.docs.size()));
        thread->queueByteSizeGauge->incBy(dataMsg.getByteSize());
        if (size > _maxQueueSizeBytes) {
            // ProducerConsumerQueue will throw ProducerConsumerQueueBatchTooLarge if a single
            // item is larger than the max queue size. So we do this to allow a single large
            // StreamDataMsg in the queue.
            size = _maxQueueSizeBytes;
        }
        if (size == 0) {
            // It's possiblefor dataMsg.getByteSize() / getApproximateSize() to return zero
            // in some unlucky cases. A size of zero causes an invariant in ProducerConsumerQueue.
            LOGV2_WARNING(9934300, "Unexpected zero size message", "context"_attr = _context);
            size = 1;
        }
        thread->queue->push(Message{.data = std::move(dataMsg), .size = size});
    };

    if (_threads.size() == 1) {
        sendMsg(_threads[0].get(), std::move(dataMsg));
    } else {
        // Scatter the documents to different threads based on a hash of their
        // $merge.on fields.

        // Split up the batch into partitions based on the writer's partitioning logic.
        auto writer = _threads.front()->writer.get();

        // Create a vector containing a message for each thread.
        std::vector<StreamDataMsg> msgs;
        msgs.reserve(_threads.size());
        const int docsSize = dataMsg.docs.size() / _threads.size();
        for (size_t i = 0; i < _threads.size(); ++i) {
            StreamDataMsg msg;
            msg.creationTimer = dataMsg.creationTimer;
            msg.docs.reserve(docsSize);
            msgs.push_back(std::move(msg));
        }

        // Partition the documents.
        for (auto& doc : dataMsg.docs) {
            size_t hash = writer->partition(doc);
            auto idx = hash % _threads.size();
            msgs[idx].docs.push_back(std::move(doc));
        }

        // Send the messages to the threads.
        for (size_t idx = 0; idx < msgs.size(); ++idx) {
            auto msg = std::move(msgs[idx]);
            if (!msg.docs.empty()) {
                sendMsg(_threads[idx].get(), std::move(msg));
            }
        }

        // TODO(SERVER-100663): Optimize the common case where all _id are generated
        // by us. In that case, we can just use insert instead of update.
    }

    if (controlMsg) {
        onControlMsg(inputIdx, std::move(*controlMsg));
    }
}

ConnectionStatus QueuedSinkOperator::doGetConnectionStatus() {
    ConnectionStatus status{ConnectionStatus::kConnected};
    for (auto& thread : _threads) {
        stdx::lock_guard<stdx::mutex> lock(thread->consumerMutex);
        auto consumerStatus = thread->consumerStatus;
        if (!consumerStatus.isConnected()) {
            return consumerStatus;
        }
    }
    return ConnectionStatus{ConnectionStatus::kConnected};
}

void QueuedSinkOperator::WriterThread::consumeLoop() {
    bool done{false};
    SPStatus status;

    StreamDataMsg batchMsg{};
    bool setCreationTimer{false};
    int64_t batchMsgDataSize{0};

    std::function<void()> sendBatchMsgFn = [&]() {
        auto stats = this->writer->processDataMsg(std::move(batchMsg));
        batchMsg = StreamDataMsg{};
        setCreationTimer = false;
        batchMsgDataSize = 0;
        stdx::lock_guard<stdx::mutex> lock(consumerMutex);
        consumerStats += stats;
    };

    while (!done) {
        try {
            auto msg = queue->tryPop();
            if (!msg) {
                if (batchMsg.docs.size() > 0) {
                    sendBatchMsgFn();
                }
                msg = queue->pop();
            }
            if (msg->flushSignal) {
                if (batchMsg.docs.size() > 0) {
                    sendBatchMsgFn();
                }
                stdx::lock_guard<stdx::mutex> lock(consumerMutex);
                pendingFlush = false;
                flushedCv.notify_all();
                continue;
            }
            batchMsg.docs.reserve(batchMsg.docs.size() + msg->data->docs.size());
            int docsHandled{0};
            for (auto& streamDoc : msg->data->docs) {
                auto docSize = streamDoc.doc.getCurrentApproximateSize();
                if (batchMsgDataSize + docSize >= kSinkDataMsgMaxByteSize ||
                    batchMsg.docs.size() + 1 == kSinkDataMsgMaxDocSize) {
                    sendBatchMsgFn();
                    batchMsg.docs.reserve(msg->data->docs.size() - docsHandled);
                }
                if (!setCreationTimer) {
                    batchMsg.creationTimer = msg->data->creationTimer;
                    setCreationTimer = true;
                }
                queueSizeGauge->incBy(-1);
                queueByteSizeGauge->incBy(-1 * docSize);
                batchMsg.docs.emplace_back(std::move(streamDoc));
                batchMsgDataSize += docSize;
                docsHandled++;
            }
        } catch (const ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>&) {
            // Closed naturally from `stop()`.
            done = true;
        } catch (const SPException& e) {
            status = e.toStatus();
            done = true;
        } catch (const DBException& e) {
            status = SPStatus(e.toStatus());
            done = true;
        } catch (const std::exception& e) {
            LOGV2_WARNING(8748301,
                          "Unexpected std::exception in queued_sink_operator",
                          "context"_attr = context,
                          "operatorName"_attr = writer->getName(),
                          "exception"_attr = e.what());
            status = {{mongo::ErrorCodes::UnknownError,
                       "An unknown error occured in " + writer->getName()},
                      e.what()};
            done = true;
        }
    }

    // This will cause any thread calling _queue.push to throw an exception.
    queue->closeConsumerEnd();

    // Wake up the executor thread if its waiting on a flush. If we're exiting the consume
    // loop because of an exception, then the flush in the executor thread will fail after
    // it receives the flushed condvar signal.
    stdx::lock_guard<stdx::mutex> lock(consumerMutex);
    if (!status.isOK()) {
        consumerStatus = ConnectionStatus{ConnectionStatus::kError, std::move(status)};
    }
    consumerThreadRunning = false;
    flushedCv.notify_all();
}
};  // namespace streams
