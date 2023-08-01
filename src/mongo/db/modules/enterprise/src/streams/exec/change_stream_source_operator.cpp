/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/change_stream_source_operator.h"

#include <bsoncxx/json.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/pipeline.hpp>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/logv2/log.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

namespace streams {

using namespace mongo;

ChangeStreamSourceOperator::ChangeStreamSourceOperator(Context* context, Options options)
    : SourceOperator(context, /*numOutputs*/ 1), _options(std::move(options)) {
    auto* svcCtx = _options.clientOptions.svcCtx;
    invariant(svcCtx);
    _instance = getMongocxxInstance(svcCtx);
    _uri = std::make_unique<mongocxx::uri>(_options.clientOptions.uri);
    _client =
        std::make_unique<mongocxx::client>(*_uri, _options.clientOptions.toMongoCxxClientOptions());

    const auto& db = _options.clientOptions.database;
    tassert(7596201, "Expected a non-empty database name", !db.empty());
    _database = std::make_unique<mongocxx::database>(_client->database(db));
    const auto& coll = _options.clientOptions.collection;
    if (!coll.empty()) {
        _collection = std::make_unique<mongocxx::collection>(_database->collection(coll));
    }
}

ChangeStreamSourceOperator::~ChangeStreamSourceOperator() {
    // '_changeStreamThread' must not be running on shutdown.
    dassert(!_changeStreamThread.joinable());
}

std::vector<mongo::BSONObj> ChangeStreamSourceOperator::getDocuments() {
    std::vector<mongo::BSONObj> docs;
    {
        stdx::lock_guard<Latch> lock(_mutex);
        // Throw '_exception' to the caller if one was raised.
        if (_exception) {
            std::rethrow_exception(_exception);
        }

        // Early return if there are no change events to return.
        if (_changeEvents.empty()) {
            return docs;
        }

        docs = std::move(_changeEvents.front());
        _changeEvents.pop();
        _numChangeEvents -= docs.size();
        _changeStreamThreadCond.notify_all();
    }
    return docs;
}

void ChangeStreamSourceOperator::doStart() {
    // Establish our change stream cursor.
    if (_collection) {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _collection->watch(mongocxx::pipeline(), _options.changeStreamOptions));
    } else {
        tassert(7596202,
                "Change stream $source is only supported against a collection or a database",
                _database);
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _database->watch(mongocxx::pipeline(), _options.changeStreamOptions));
    }
    _it = mongocxx::change_stream::iterator();

    // Start the thread that will be reading from '_changeStreamCursor' via '_it'.
    dassert(!_changeStreamThread.joinable());
    _changeStreamThread = stdx::thread([this] { fetchLoop(); });
}

void ChangeStreamSourceOperator::doStop() {
    // Stop the consumer thread.
    bool joinThread{false};
    if (_changeStreamThread.joinable()) {
        stdx::unique_lock lock(_mutex);
        joinThread = true;
        _shutdown = true;
        _changeStreamThreadCond.notify_one();
    }
    if (joinThread) {
        // Wait for the consumer thread to exit.
        _changeStreamThread.join();
    }

    // Destroy our iterator and close our cursor.
    _it = mongocxx::change_stream::iterator();
    _changeStreamCursor = nullptr;
}

int32_t ChangeStreamSourceOperator::doRunOnce() {
    auto changeEvents = getDocuments();
    dassert(int32_t(changeEvents.size()) <= _options.maxNumDocsToReturn);

    // Return if no documents are available at the moment.
    if (changeEvents.empty()) {
        return 0;
    }

    StreamDataMsg dataMsg;
    int64_t totalNumInputBytes = 0;

    for (auto& changeEvent : changeEvents) {
        int64_t inputBytes = changeEvent.objsize();

        if (auto streamDoc = processChangeEvent(std::move(changeEvent)); streamDoc) {
            dataMsg.docs.push_back(std::move(*streamDoc));
            totalNumInputBytes += inputBytes;
        }
    }

    // Early return if we did not manage to add any change events to 'dataMsg.docs'.
    if (dataMsg.docs.empty()) {
        return 0;
    }

    boost::optional<StreamControlMsg> newControlMsg = boost::none;
    if (_options.watermarkGenerator) {
        newControlMsg = StreamControlMsg{_options.watermarkGenerator->getWatermarkMsg()};
        if (*newControlMsg == _lastControlMsg) {
            newControlMsg = boost::none;
        } else {
            _lastControlMsg = *newControlMsg;
        }
    }

    incOperatorStats(OperatorStats{.numInputDocs = int64_t(dataMsg.docs.size()),
                                   .numInputBytes = totalNumInputBytes});
    int32_t docsSent = dataMsg.docs.size();
    sendDataMsg(0, std::move(dataMsg), std::move(newControlMsg));
    return docsSent;
}

void ChangeStreamSourceOperator::fetchLoop() {
    int32_t numDocsToFetch{0};
    while (true) {
        {
            stdx::unique_lock lock(_mutex);
            if (_shutdown) {
                LOGV2_INFO(7788500, "Change stream $source exiting fetchLoop()");
                break;
            }

            if (numDocsToFetch <= 0) {
                if (_numChangeEvents < _options.maxNumDocsToPrefetch) {
                    numDocsToFetch = _options.maxNumDocsToPrefetch - _numChangeEvents;
                } else {
                    LOGV2_DEBUG(
                        7788501,
                        1,
                        "Change stream $source sleeping when numChangeEvents: {numChangeEvents}",
                        "numChangeEvents"_attr = _numChangeEvents);
                    _changeStreamThreadCond.wait(lock, [this]() {
                        return _shutdown || _numChangeEvents < _options.maxNumDocsToPrefetch;
                    });
                    LOGV2_DEBUG(7788502,
                                1,
                                "Change stream $source waking up when numDocs: {numChangeEvents}",
                                "numChangeEvents"_attr = _numChangeEvents);
                }
            }
        }

        // Get some change events from our change stream cursor.
        if (readSingleChangeEvent()) {
            LOGV2_DEBUG(7788503, 1, "Change stream $source: cursor fetched 1 change event");
            --numDocsToFetch;
        }
    }
}

bool ChangeStreamSourceOperator::readSingleChangeEvent() {
    // TODO SERVER-77657: Handle invalidate events.
    invariant(_changeStreamCursor);

    boost::optional<mongo::BSONObj> changeEventObj;
    try {
        // See if there are any available notifications. Note that '_changeStreamCursor->begin()'
        // will return the next available notification (that is, it will not reset our cursor to the
        // very beginning).
        if (_it == mongocxx::change_stream::iterator()) {
            _it = _changeStreamCursor->begin();
        }

        // If our cursor is exhausted, wait until the next call to 'readSingleChangeEvent' to try
        // reading from '_changeStreamCursor' again.
        if (_it != _changeStreamCursor->end()) {
            // TODO SERVER-77563: This conversion from bsoncxx to BSONObj can be improved.
            changeEventObj = mongo::fromjson(bsoncxx::to_json(*_it));

            // Advance our cursor before processing the current document.
            ++_it;
        }
    } catch (const std::exception& e) {
        LOGV2_ERROR(7788504,
                    "Change stream $source encountered exception: {error}",
                    "error"_attr = e.what());
        {
            stdx::lock_guard<Latch> lock(_mutex);
            _shutdown = true;
            if (!_exception) {
                _exception = std::current_exception();
            }
        }
    }

    // If we've hit the end of our cursor, set our iterator to the default iterator so that we can
    // reset it on the next call to 'readSingleChangeEvent'.
    if (_it == _changeStreamCursor->end()) {
        _it = mongocxx::change_stream::iterator();
    }

    // Return early if we didn't read a change event from our cursor.
    if (!changeEventObj) {
        return false;
    }

    {
        stdx::lock_guard<Latch> lock(_mutex);
        const auto capacity = _options.maxNumDocsToReturn;
        // Create a new vector if none exist or if the last vector is full.
        if (_changeEvents.empty() || int32_t(_changeEvents.back().size()) == capacity) {
            _changeEvents.emplace();
            _changeEvents.back().reserve(capacity);
        }

        _changeEvents.back().emplace_back(std::move(*changeEventObj));
        ++_numChangeEvents;
    }

    return true;
}

// Obtain the 'ts' field from either:
// - 'timeField' if the user specified
// - The 'wallTime' field (if we're reading from a change stream against a cluster whose
// version is GTE 6.0).
// - The 'clusterTime' (if we're reading from a change stream against a cluster whose
// version is LT 6.0).
//
// Then, does additional work to generate a watermark. Throws if a timestamp could not be obtained.
mongo::Date_t ChangeStreamSourceOperator::getTimestamp(const Document& changeEventDoc) {
    mongo::Date_t ts;
    if (_options.timestampExtractor) {
        ts = _options.timestampExtractor->extractTimestamp(changeEventDoc);
    } else if (auto wallTime = changeEventDoc[DocumentSourceChangeStream::kWallTimeField];
               !wallTime.missing()) {
        uassert(7926400,
                "Change event's wall time was not a date",
                wallTime.getType() == BSONType::Date);
        ts = wallTime.getDate();
    } else {
        auto clusterTime = changeEventDoc[DocumentSourceChangeStream::kClusterTimeField];
        uassert(7926401, "Change event did not have clusterTime", !clusterTime.missing());
        uassert(7926402,
                "clusterTime for change event was not a timestamp",
                clusterTime.getType() == BSONType::bsonTimestamp);
        ts = Date_t::fromMillisSinceEpoch(clusterTime.getTimestamp().asInt64());
    }

    if (_options.watermarkGenerator) {
        uassert(7926403,
                "Change event arrived late",
                !_options.watermarkGenerator->isLate(ts.toMillisSinceEpoch()));
        _options.watermarkGenerator->onEvent(ts.toMillisSinceEpoch());
    }

    return ts;
}

boost::optional<StreamDocument> ChangeStreamSourceOperator::processChangeEvent(
    mongo::BSONObj changeStreamObj) {
    Document changeEventDoc(std::move(changeStreamObj));
    mongo::Date_t ts;

    // If an exception is thrown when trying to get a timestamp, DLQ 'changeEventDoc' and return.
    try {
        ts = getTimestamp(changeEventDoc);
    } catch (const DBException& e) {
        _context->dlq->addMessage(toDeadLetterQueueMsg(std::move(changeEventDoc), e.toString()));
        return boost::none;
    }

    MutableDocument mutableChangeEvent(std::move(changeEventDoc));

    // Add 'ts' to 'mutableChangeEvent', overwriting 'timestampOutputFieldName' if it already
    // exists.
    mutableChangeEvent[_options.timestampOutputFieldName] = Value(ts);

    StreamDocument streamDoc(mutableChangeEvent.freeze());

    streamDoc.streamMeta.setSourceType(StreamMetaSourceTypeEnum::Atlas);
    streamDoc.streamMeta.setTimestamp(ts);

    streamDoc.minProcessingTimeMs = curTimeMillis64();
    streamDoc.minEventTimestampMs = ts.toMillisSinceEpoch();
    streamDoc.maxEventTimestampMs = ts.toMillisSinceEpoch();
    return streamDoc;
}
}  // namespace streams
