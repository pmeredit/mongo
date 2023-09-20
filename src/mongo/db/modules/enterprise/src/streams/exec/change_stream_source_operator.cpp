/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/change_stream_source_operator.h"

#include <bsoncxx/json.hpp>
#include <chrono>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/pipeline.hpp>
#include <variant>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/log_util.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

void ChangeStreamSourceOperator::DocBatch::pushDoc(mongo::BSONObj doc) {
    byteSize += doc.objsize();
    docs.push_back(std::move(doc));
}

int64_t ChangeStreamSourceOperator::DocBatch::getByteSize() const {
    dassert(byteSize >= 0);
    return byteSize;
}

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

    if (_options.userSpecifiedStartingPoint) {
        // The user has supplied a resumeToken for us to start after,
        // or a clusterTime for us to start at. The startingPoint
        // may also be modified in initFromCheckpoint().
        _state.setStartingPoint(*_options.userSpecifiedStartingPoint);
    }

    if (_options.useWatermarks) {
        _watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(
            0 /* inputIdx */, nullptr /* combiner */, _options.allowedLatenessMs);
    }

    _changeStreamOptions.full_document(
        FullDocumentMode_serializer(_options.fullDocumentMode).toString());
}

ChangeStreamSourceOperator::~ChangeStreamSourceOperator() {
    // '_changeStreamThread' must not be running on shutdown.
    dassert(!_changeStreamThread.joinable());
}

ChangeStreamSourceOperator::DocBatch ChangeStreamSourceOperator::getDocuments() {
    stdx::lock_guard<Latch> lock(_mutex);
    // Throw '_exception' to the caller if one was raised.
    if (_exception) {
        std::rethrow_exception(_exception);
    }

    // Early return if there are no change events to return.
    if (_changeEvents.empty()) {
        return DocBatch(/*capacity*/ 0);
    }

    auto batch = std::move(_changeEvents.front());
    _changeEvents.pop();
    _numChangeEvents -= batch.size();
    _changeStreamThreadCond.notify_all();
    return batch;
}

void ChangeStreamSourceOperator::doStart() {
    if (_context->restoreCheckpointId) {
        initFromCheckpoint();
    }

    tassert(7596202, "_database should be set", _database);
    // Run the ping command to test the connection and retrieve the current operationTime.
    // A failure will throw an exception.
    auto pingResponse = _database->run_command(make_document(kvp("ping", "1")));
    if (!_state.getStartingPoint()) {
        // If we don't have a starting point, use the operationTime from the ping request.
        auto timestamp = pingResponse["operationTime"].get_timestamp();
        _state.setStartingPoint(stdx::variant<mongo::BSONObj, mongo::Timestamp>(
            Timestamp{timestamp.timestamp, timestamp.increment}));
    }

    // Establish our change stream cursor.
    // The startingPoint may be set in constructor, in initFromCheckpoint(),
    // or using the operationTimestamp from the ping command above.
    if (stdx::holds_alternative<BSONObj>(*_state.getStartingPoint())) {
        const auto& resumeToken = std::get<BSONObj>(*_state.getStartingPoint());
        LOGV2_INFO(7788511,
                   "Changestream $source starting with startAfter",
                   "resumeToken"_attr = tojson(resumeToken),
                   "context"_attr = _context);
        _changeStreamOptions.start_after(toBsoncxxDocument(resumeToken));
    } else {
        invariant(stdx::holds_alternative<Timestamp>(*_state.getStartingPoint()));
        auto timestamp = std::get<Timestamp>(*_state.getStartingPoint());
        LOGV2_INFO(7788513,
                   "Changestream $source starting with startAtOperationTime",
                   "timestamp"_attr = timestamp,
                   "context"_attr = _context);
        _changeStreamOptions.start_at_operation_time(bsoncxx::types::b_timestamp{
            .increment = timestamp.getInc(), .timestamp = timestamp.getSecs()});
    }

    if (_collection) {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _collection->watch(mongocxx::pipeline(), _changeStreamOptions));
    } else {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _database->watch(mongocxx::pipeline(), _changeStreamOptions));
    }

    _it = mongocxx::change_stream::iterator();

    // Start the background producer thread that will be reading from '_changeStreamCursor' via
    // '_it'.
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

int64_t ChangeStreamSourceOperator::doRunOnce() {
    auto batch = getDocuments();
    auto& changeEvents = batch.docs;
    dassert(int32_t(changeEvents.size()) <= _options.maxNumDocsToReturn);

    // Return if no documents are available at the moment.
    if (changeEvents.empty()) {
        return 0;
    }

    StreamDataMsg dataMsg;
    int64_t totalNumInputDocs = changeEvents.size();
    int64_t totalNumInputBytes = 0;
    int64_t numDlqDocs = 0;

    for (auto& changeEvent : changeEvents) {
        size_t inputBytes = changeEvent.objsize();
        totalNumInputBytes += inputBytes;

        if (auto streamDoc = processChangeEvent(std::move(changeEvent)); streamDoc) {
            dataMsg.docs.push_back(std::move(*streamDoc));
        } else {
            ++numDlqDocs;
        }
    }

    // Early return if we did not manage to add any change events to 'dataMsg.docs'.
    if (dataMsg.docs.empty()) {
        return 0;
    }

    boost::optional<StreamControlMsg> newControlMsg = boost::none;
    if (_watermarkGenerator) {
        newControlMsg = StreamControlMsg{_watermarkGenerator->getWatermarkMsg()};
        if (*newControlMsg == _lastControlMsg) {
            newControlMsg = boost::none;
        } else {
            _lastControlMsg = *newControlMsg;
        }
    }

    incOperatorStats(OperatorStats{.numInputDocs = totalNumInputDocs,
                                   .numInputBytes = totalNumInputBytes,
                                   .numDlqDocs = numDlqDocs});
    sendDataMsg(0, std::move(dataMsg), std::move(newControlMsg));
    tassert(7788508, "Expected resume token in batch", batch.lastResumeToken);
    _state.setStartingPoint(
        stdx::variant<mongo::BSONObj, mongo::Timestamp>(std::move(*batch.lastResumeToken)));
    LOGV2_DEBUG(7788507,
                2,
                "Change stream $source: updated resume token",
                "context"_attr = _context,
                "resumeToken"_attr = tojson(std::get<BSONObj>(*_state.getStartingPoint())));

    return totalNumInputDocs;
}

void ChangeStreamSourceOperator::fetchLoop() {
    int32_t numDocsToFetch{0};
    while (true) {
        {
            stdx::unique_lock lock(_mutex);
            if (_shutdown) {
                LOGV2_INFO(7788500,
                           "Change stream $source exiting fetchLoop()",
                           "context"_attr = _context);
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
                        "context"_attr = _context,
                        "numChangeEvents"_attr = _numChangeEvents);
                    _changeStreamThreadCond.wait(lock, [this]() {
                        return _shutdown || _numChangeEvents < _options.maxNumDocsToPrefetch;
                    });
                    LOGV2_DEBUG(7788502,
                                1,
                                "Change stream $source waking up when numDocs: {numChangeEvents}",
                                "context"_attr = _context,
                                "numChangeEvents"_attr = _numChangeEvents);
                }
            }
        }

        // Get some change events from our change stream cursor.
        if (readSingleChangeEvent()) {
            tassert(
                7788509, "Expected resume token in batch", _changeEvents.back().lastResumeToken);
            LOGV2_DEBUG(7788503,
                        2,
                        "Change stream $source: cursor fetched 1 change event",
                        "context"_attr = _context,
                        "resumeToken"_attr = tojson(*_changeEvents.back().lastResumeToken));
            --numDocsToFetch;
        }
    }
}

bool ChangeStreamSourceOperator::readSingleChangeEvent() {
    // TODO SERVER-77657: Handle invalidate events.
    invariant(_changeStreamCursor);

    boost::optional<mongo::BSONObj> changeEvent;
    boost::optional<mongo::BSONObj> eventResumeToken;
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
            changeEvent = fromBsonCxxDocument(*_it);
            // From the mongocxx documentation:
            // "Once this change stream has been iterated,
            // this method will return the resume token of the most recently returned document in
            // the stream, or a postDocBatchResumeToken if the current batch of documents has been
            // exhausted." Since we've iterated the stream, we can always expect a resumeToken from
            // this method.
            auto resumeToken = _changeStreamCursor->get_resume_token();
            tassert(7788510, "Expected resume token from cursor", resumeToken);
            eventResumeToken = fromBsonCxxDocument(std::move(*resumeToken));

            // Advance our cursor before processing the current document.
            ++_it;
        }
    } catch (const std::exception& e) {
        LOGV2_ERROR(7788504,
                    "Change stream $source encountered exception: {error}",
                    "context"_attr = _context,
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
    if (!changeEvent) {
        return false;
    }

    {
        stdx::lock_guard<Latch> lock(_mutex);
        const auto capacity = _options.maxNumDocsToReturn;
        // Create a new vector if none exist or if the last vector is full.
        if (_changeEvents.empty() || int32_t(_changeEvents.back().size()) == capacity ||
            _changeEvents.back().getByteSize() >= kDataMsgMaxByteSize) {
            _changeEvents.emplace(capacity);
        }

        _changeEvents.back().pushDoc(std::move(*changeEvent));
        _changeEvents.back().lastResumeToken = std::move(*eventResumeToken);
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

    if (_watermarkGenerator) {
        uassert(7926403,
                "Change event arrived late",
                !_watermarkGenerator->isLate(ts.toMillisSinceEpoch()));
        _watermarkGenerator->onEvent(ts.toMillisSinceEpoch());
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

void ChangeStreamSourceOperator::initFromCheckpoint() {
    invariant(_context->restoreCheckpointId);
    boost::optional<mongo::BSONObj> bsonState = _context->checkpointStorage->readState(
        *_context->restoreCheckpointId, _operatorId, 0 /* chunkNumber */);
    CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                               _operatorId,
                               "expected bson state for changestream $source",
                               bsonState);
    _state = ChangeStreamSourceCheckpointState::parseOwned(
        IDLParserContext("ChangeStreamSourceOperator"), std::move(*bsonState));

    CHECKPOINT_RECOVERY_ASSERT(
        *_context->restoreCheckpointId,
        _operatorId,
        fmt::format("state has unexpected watermark: {}", bool(_state.getWatermark())),
        bool(_state.getWatermark()) == _options.useWatermarks);
    if (_options.useWatermarks) {
        // All watermarks start as active when restoring from a checkpoint.
        WatermarkControlMsg watermark{WatermarkStatus::kActive,
                                      _state.getWatermark()->getEventTimeMs()};
        _watermarkGenerator =
            std::make_unique<DelayedWatermarkGenerator>(0, /* inputIdx */
                                                        nullptr /* combiner */,
                                                        _options.allowedLatenessMs,
                                                        std::move(watermark));
    }
    LOGV2_INFO(7788505, "Change stream $source restored", "state"_attr = tojson(_state.toBSON()));
}

void ChangeStreamSourceOperator::doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) {
    invariant(controlMsg.checkpointMsg && !controlMsg.watermarkMsg);
    CheckpointId checkpointId = controlMsg.checkpointMsg->id;

    tassert(7788512,
            "Change stream $source: expected a starting point during checkpoint",
            _state.getStartingPoint());

    if (_watermarkGenerator) {
        _state.setWatermark(
            WatermarkState{_watermarkGenerator->getWatermarkMsg().eventTimeWatermarkMs});
    }

    _context->checkpointStorage->addState(checkpointId, _operatorId, _state.toBSON(), 0);
    LOGV2_INFO(7788506,
               "Change stream $source: added state",
               "checkpointId"_attr = checkpointId,
               "state"_attr = tojson(_state.toBSON()));
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

}  // namespace streams
