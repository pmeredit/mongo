/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"

#include "streams/exec/output_sampler.h"

namespace streams {

using namespace mongo;

OutputSampler::OutputSampler(Options options) : _options(std::move(options)) {
    dassert(_options.maxDocsToSample > 0);
    dassert(_options.maxBytesToSample > 0);
    _getNextCallTimestamp = Date_t::now();
}

void OutputSampler::addDataMsg(const StreamDataMsg& dataMsg) {
    stdx::lock_guard<Latch> lock(_mutex);
    int32_t numDocs = _numDocsSampled;
    int32_t numBytes = _numBytesSampled;

    std::vector<BSONObj> outputDocs;
    outputDocs.reserve(dataMsg.docs.size());
    bool done{false};
    // Append docs in dataMsg to outputDocs.
    for (size_t i = 0; i < dataMsg.docs.size(); ++i) {
        auto doc = dataMsg.docs[i].doc.toBson();
        ++numDocs;
        numBytes += doc.objsize();
        outputDocs.push_back(std::move(doc));

        if (numDocs >= _options.maxDocsToSample || numBytes >= _options.maxBytesToSample) {
            done = true;
            break;
        }
    }

    _outputDocs.push(std::move(outputDocs));
    _numDocsSampled = numDocs;
    _numBytesSampled = numBytes;
    _doneSampling = done;
}

std::vector<mongo::BSONObj> OutputSampler::getNext(int32_t batchSize) {
    std::vector<BSONObj> outputDocs;
    outputDocs.reserve(batchSize);

    stdx::lock_guard<Latch> lock(_mutex);
    _getNextCallTimestamp = Date_t::now();

    int32_t numDocsNeeded{batchSize};
    while (numDocsNeeded > 0 && !_outputDocs.empty()) {
        auto& nextBatch = _outputDocs.front();
        if (int32_t(nextBatch.size()) <= numDocsNeeded) {
            numDocsNeeded -= nextBatch.size();
            // Move all the docs in nextBatch to outputDocs.
            outputDocs.insert(outputDocs.end(),
                              std::make_move_iterator(nextBatch.begin()),
                              std::make_move_iterator(nextBatch.end()));
            _outputDocs.pop();  // We used the entire batch.
        } else {
            // Move only numDocsNeeded docs from nextBatch to outputDocs.
            outputDocs.insert(outputDocs.end(),
                              std::make_move_iterator(nextBatch.begin()),
                              std::make_move_iterator(nextBatch.begin() + numDocsNeeded));
            nextBatch.erase(nextBatch.begin(), nextBatch.begin() + numDocsNeeded);
            numDocsNeeded = 0;
        }
    }

    if (_outputDocs.empty() && _doneSampling) {
        _exhausted = true;
    }

    return outputDocs;
}

void OutputSampler::cancel() {
    stdx::lock_guard<Latch> lock(_mutex);
    _isCancelled = true;
}

bool OutputSampler::doneSampling() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _doneSampling;
}

bool OutputSampler::done() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _exhausted;
}

mongo::Date_t OutputSampler::getNextCallTimestamp() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _getNextCallTimestamp;
}

bool OutputSampler::isCancelled() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _isCancelled;
}

}  // namespace streams
