/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <queue>

#include "mongo/platform/mutex.h"
#include "mongo/util/concurrency/with_lock.h"
#include "mongo/util/time_support.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"

namespace streams {

// Abstract source operator for generated data use cases (e.g. solar sample data or in-memory data
// for testing purposes).
class GeneratedDataSourceOperator : public SourceOperator {
public:
    GeneratedDataSourceOperator(Context* context, int32_t numOutputs);

protected:
    // Guards each `run()` instance, including `getMessages()`.
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("GeneratedDataSourceOperator::mutex");

private:
    // Returns the next batch of messages to process. This is called once per `run()`.
    virtual std::vector<StreamMsgUnion> getMessages(mongo::WithLock) = 0;

    // Initializes the operator based on `getOptions()`.
    void doStart() override;

    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override {
        MONGO_UNREACHABLE;
    }
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override {
        sendControlMsg(0, std::move(controlMsg));
    }

    // Processes and forwards the messages retrieved from `getMessages()` to the next
    // operator in the pipeline.
    int64_t doRunOnce() override;

    // Determines the event timestamp associated to the document and then validates the document.
    // If the document is invalid (e.g. too late), this will return none with the error message
    // populated in the input `err` parameter, otherwise this will return the modified document.
    boost::optional<StreamDocument> processDocument(StreamDocument doc);

    // Extracts the timestamp from the input document.
    mongo::Date_t getTimestamp(const StreamDocument& doc) const;

    // Watermark generator. Only set if watermarking is enabled.
    std::unique_ptr<DelayedWatermarkGenerator> _watermarkGenerator;
};  // class GeneratedDataSourceOperator

};  // namespace streams
