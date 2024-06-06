/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <queue>

#include "mongo/platform/mutex.h"
#include "mongo/util/intrusive_counter.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * This class can be used to receive output documents from a SinkOperator to serve sample() request.
 * This class is thread-safe.
 */
class OutputSampler : public mongo::RefCountable {
public:
    struct Options {
        // Maximum number of documents to sample.
        int32_t maxDocsToSample{0};
        // Maximum number of bytes to sample.
        int32_t maxBytesToSample{0};
    };

    OutputSampler(Options options);

    // This should be called by a SinkOperator in its doOnDataMsg() method so that the sampler
    // can track stream output documents. This should only be called when doneSampling() is true.
    // This should only be called by one thread at a time.
    void addDataMsg(const StreamDataMsg& dataMsg);

    // Get the next set of output documents.
    std::vector<mongo::BSONObj> getNext(int32_t batchSize);

    // Returns the timestamp for the last getNext() call.
    mongo::Date_t getNextCallTimestamp() const;

    // Whether the sampler is done sampling.
    bool doneSampling() const;

    // Whether the sampler is done sampling and returned all the data.
    bool done() const;

    // Mark the sampler cancelled.
    void cancel();

    // Whether the sampler has been cancelled.
    bool isCancelled() const;

private:
    Options _options;
    mutable mongo::Mutex _mutex = MONGO_MAKE_LATCH("OutputSampler::mutex");
    // Number of documents received via addDataMsg() so far.
    int32_t _numDocsSampled{0};
    // Number of bytes received via addDataMsg() so far.
    int32_t _numBytesSampled{0};
    // Whether the sampler is done sampling.
    bool _doneSampling{false};
    // Whether the sampler is done returning samples. This is different from _doneSampling
    // which is marked true when the limit is hit on collection. This _exhausted flag is
    // set when all the sampled documents are all returned to the user.
    bool _exhausted{false};
    // Whether the sampler has been cancelled.
    bool _isCancelled{false};
    // Buffers all the documents received via addDataMsg() until they are returned to the
    // caller via getNext();
    std::queue<std::vector<mongo::BSONObj>> _outputDocs;
    // The timestamp at which getNext() was last called.
    mongo::Date_t _getNextCallTimestamp;
};

}  // namespace streams
