/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/change_stream_source_operator.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/pipeline/document_source_change_stream_gen.h"
#include "streams/exec/message.h"
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <chrono>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/pipeline.hpp>
#include <variant>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/log_util.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

using namespace mongo;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

namespace {

// Name of the error code field name in the raw server error object.
static constexpr char kErrorCodeFieldName[] = "code";

};  // namespace

int ChangeStreamSourceOperator::DocBatch::pushDoc(mongo::BSONObj doc) {
    int docSize = doc.objsize();
    byteSize += docSize;
    docs.push_back(std::move(doc));
    return docSize;
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
    _uri = makeMongocxxUri(_options.clientOptions.uri);
    _client =
        std::make_unique<mongocxx::client>(*_uri, _options.clientOptions.toMongoCxxClientOptions());

    const auto& db = _options.clientOptions.database;
    tassert(7596201, "Expected a non-empty database name", db && !db->empty());
    _database = std::make_unique<mongocxx::database>(_client->database(*db));
    const auto& coll = _options.clientOptions.collection;
    if (coll && !coll->empty()) {
        _collection = std::make_unique<mongocxx::collection>(_database->collection(*coll));
    }

    if (_options.userSpecifiedStartingPoint) {
        // The user has supplied a resumeToken for us to start after,
        // or a clusterTime for us to start at. The startingPoint
        // may also be modified in initFromCheckpoint().
        _state.setStartingPoint(*_options.userSpecifiedStartingPoint);
    }

    if (_options.useWatermarks) {
        _watermarkGenerator =
            std::make_unique<DelayedWatermarkGenerator>(0 /* inputIdx */, nullptr /* combiner */);
    }

    _changeStreamOptions.full_document(
        FullDocumentMode_serializer(_options.fullDocumentMode).toString());
    _changeStreamOptions.full_document_before_change(
        FullDocumentBeforeChangeMode_serializer(_options.fullDocumentBeforeChangeMode).toString());

    if (!_options.clientOptions.collectionList.empty()) {
        auto arrayBuilder = bsoncxx::builder::basic::array{};
        for (const auto& element : _options.clientOptions.collectionList) {
            arrayBuilder.append(element);
        }
        _pipeline.match(
            make_document(kvp("ns.coll", make_document(kvp("$in", arrayBuilder.view())))));
    }
    for (const auto& stage : _options.pipeline) {
        _pipeline.append_stage(toBsoncxxView(stage));
    }
}

ChangeStreamSourceOperator::~ChangeStreamSourceOperator() {
    // '_changeStreamThread' must not be running on shutdown.
    dassert(!_changeStreamThread.joinable());
}

OperatorStats ChangeStreamSourceOperator::doGetStats() {
    OperatorStats stats{_stats};
    {
        stdx::lock_guard<Latch> lock(_mutex);
        stats += _consumerStats;

        // Always expose memory usage from the memory usage handle so that
        // the sum of each operator's memory usage equals the stream processor's
        // memory aggregator memory usage.
        stats.memoryUsageBytes = _memoryUsageHandle.getCurrentMemoryUsageBytes();
    }

    return stats;
}

void ChangeStreamSourceOperator::registerMetrics(MetricManager* metricManager) {
    _queueSizeGauge = metricManager->registerIntGauge(
        "source_operator_queue_size",
        /* description */ "Total docs currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context));
    _queueByteSizeGauge = metricManager->registerIntGauge(
        "source_operator_queue_bytesize",
        /* description */ "Total bytes currently buffered in the queue",
        /*labels*/ getDefaultMetricLabels(_context));
}

ChangeStreamSourceOperator::DocBatch ChangeStreamSourceOperator::getDocuments() {
    stdx::lock_guard<Latch> lock(_mutex);
    // Throw '_exception' to the caller if one was raised.
    if (_exception) {
        uasserted(8112601,
                  fmt::format(
                      "Error encountered while reading from change stream $source for db: "
                      "{} and collection: {}",
                      _options.clientOptions.database ? *_options.clientOptions.database : "",
                      _options.clientOptions.collection ? *_options.clientOptions.collection : ""));
    }

    // Early return if there are no change events to return.
    if (_changeEvents.empty()) {
        return DocBatch(/*capacity*/ 0);
    }

    auto batch = std::move(_changeEvents.front());
    tassert(7788509, "Expected resume token in batch", batch.lastResumeToken);
    LOGV2_DEBUG(7788503,
                2,
                "Change stream $source: processing a batch of events",
                "context"_attr = _context,
                "resumeToken"_attr = tojson(*batch.lastResumeToken));
    _changeEvents.pop();
    _queueSizeGauge->incBy(-batch.size());
    _queueByteSizeGauge->incBy(-batch.getByteSize());
    _consumerStats += {.memoryUsageBytes = -batch.getByteSize()};
    _memoryUsageHandle.set(_consumerStats.memoryUsageBytes);
    _changeStreamThreadCond.notify_all();
    return batch;
}

void pushdownPipeline(mongocxx::pipeline& cxxPipeline, const std::vector<BSONObj>& pipeline) {
    for (const auto& stage : pipeline) {
        cxxPipeline.append_stage(toBsoncxxView(stage));
    }
}

void ChangeStreamSourceOperator::connectToSource() {
    if (_context->restoreCheckpointId) {
        initFromCheckpoint();
    }

    // Run the hello command to test the connection and retrieve the current operationTime.
    // A failure will throw an exception.
    auto helloResponse = _database->run_command(make_document(kvp("hello", "1")));
    if (!_state.getStartingPoint()) {
        // If we don't have a starting point, use the operationTime from the hello request.
        auto operationTime = helloResponse["operationTime"];
        uassert(8308700,
                "Expected an operationTime timestamp field.",
                operationTime.type() == bsoncxx::type::k_timestamp);
        auto timestamp = operationTime.get_timestamp();
        _state.setStartingPoint(std::variant<mongo::BSONObj, mongo::Timestamp>(
            Timestamp{timestamp.timestamp, timestamp.increment}));
    }

    // Establish our change stream cursor.
    // The startingPoint may be set in constructor, in initFromCheckpoint(),
    // or using the operationTimestamp from the ping command above.
    if (holds_alternative<BSONObj>(*_state.getStartingPoint())) {
        const auto& resumeToken = get<BSONObj>(*_state.getStartingPoint());
        LOGV2_INFO(7788511,
                   "Changestream $source starting with startAfter",
                   "resumeToken"_attr = tojson(resumeToken),
                   "context"_attr = _context);
        _changeStreamOptions.start_after(toBsoncxxView(resumeToken));
    } else {
        invariant(holds_alternative<Timestamp>(*_state.getStartingPoint()));
        auto timestamp = get<Timestamp>(*_state.getStartingPoint());
        LOGV2_INFO(7788513,
                   "Changestream $source starting with startAtOperationTime",
                   "timestamp"_attr = timestamp,
                   "context"_attr = _context);
        _changeStreamOptions.start_at_operation_time(bsoncxx::types::b_timestamp{
            .increment = timestamp.getInc(), .timestamp = timestamp.getSecs()});
    }

    if (_collection) {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _collection->watch(_pipeline, _changeStreamOptions));
    } else {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _database->watch(_pipeline, _changeStreamOptions));
    }

    _it = mongocxx::change_stream::iterator();
}

void ChangeStreamSourceOperator::fetchLoop() {
    auto dbName = _options.clientOptions.database ? *_options.clientOptions.database : "";
    auto collName = _options.clientOptions.collection ? *_options.clientOptions.collection : "";
    auto genericErrorMessage = fmt::format(
        "Failed to connect to change stream $source for db: "
        "{} and collection: {}",
        dbName,
        collName);

    auto fetchFunc = [this]() {
        // Establish the connection and start the changestream.
        connectToSource();
        {
            stdx::unique_lock lock(_mutex);
            _connectionStatus = {ConnectionStatus::kConnected};
        }

        // Start reading events in a loop.
        while (true) {
            {
                stdx::unique_lock lock(_mutex);
                if (_shutdown) {
                    LOGV2_INFO(7788500,
                               "Change stream $source exiting fetchLoop()",
                               "context"_attr = _context);
                    break;
                }

                if (_consumerStats.memoryUsageBytes >= _options.maxPrefetchByteSize) {
                    LOGV2_DEBUG(7788501,
                                1,
                                "Change stream $source sleeping when bytesBuffered: "
                                "{bytesBuffered}",
                                "context"_attr = _context,
                                "bytesBuffered"_attr = _consumerStats.memoryUsageBytes);
                    _changeStreamThreadCond.wait(lock, [this]() {
                        // Wait until either the SP is shutdown or there is capacity for events
                        // in the last DocVec.
                        return _shutdown ||
                            _consumerStats.memoryUsageBytes < _options.maxPrefetchByteSize;
                    });
                    LOGV2_DEBUG(
                        7788502,
                        1,
                        "Change stream $source waking up when bytesBuffered: {bytesBuffered}",
                        "context"_attr = _context,
                        "bytesBuffered"_attr = _consumerStats.memoryUsageBytes);
                }
            }

            // Get some change events from our change stream cursor.
            readSingleChangeEvent();
        }
    };
    auto status = runMongocxxNoThrow(
        std::move(fetchFunc), _context, ErrorCodes::Error{8681500}, genericErrorMessage, *_uri);

    if (status.code() == ErrorCodes::BSONObjectTooLarge) {
        // In SERVER-87592 we realized a pipeline like [$source: {db: test}, $merge: {into: {db:
        // test}}] will create an infinite loop. The loop creates a document that gets larger and
        // larger. This will eventually lead to an error from the _changestream server_, complaining
        // 16MB limit is exceeded. In this case we fail the stream processor with a non-retryable
        // error.
        status = {{ErrorCodes::BSONObjectTooLarge,
                   "A changestream $source event larger than 16MB was detected."},
                  ""};
    }

    // If the status returned is not OK, set the error in connectionStatus.
    if (!status.isOK()) {
        stdx::unique_lock lock(_mutex);
        _connectionStatus = {ConnectionStatus::kError, std::move(status)};
    }
}

ConnectionStatus ChangeStreamSourceOperator::doGetConnectionStatus() {
    ConnectionStatus status;
    {
        stdx::unique_lock lock(_mutex);
        status = _connectionStatus;
    }
    return status;
}

void ChangeStreamSourceOperator::doStart() {
    if (_context->restoreCheckpointId) {
        boost::optional<mongo::BSONObj> bsonState;
        if (_context->oldCheckpointStorage) {
            bsonState = _context->oldCheckpointStorage->readState(
                *_context->restoreCheckpointId, _operatorId, 0 /* chunkNumber */);
        } else {
            invariant(_context->checkpointStorage);
            auto reader = _context->checkpointStorage->createStateReader(
                *_context->restoreCheckpointId, _operatorId);
            auto record = _context->checkpointStorage->getNextRecord(reader.get());
            CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                       _operatorId,
                                       "expected state for changestream $source",
                                       record);
            bsonState = record->toBson();
        }
        CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                   _operatorId,
                                   "expected bson state for changestream $source",
                                   bsonState);
        _restoreCheckpointState = ChangeStreamSourceCheckpointState::parseOwned(
            IDLParserContext("ChangeStreamSourceOperator"), std::move(*bsonState));
    }

    invariant(_database);
    invariant(!_changeStreamThread.joinable());
    _changeStreamThread = stdx::thread([this]() {
        try {
            fetchLoop();
        } catch (const std::exception& e) {
            // Note: fetchLoop has its own error handling so we don't expect to this this exception.
            LOGV2_WARNING(8681501,
                          "Unexpected std::exception in changestream $source",
                          "exception"_attr = e.what());
            stdx::unique_lock lock(_mutex);
            _connectionStatus = {
                ConnectionStatus::kError,
                {{ErrorCodes::InternalError, "Unexpected exception in changestream $source."},
                 e.what()}};
        }
    });
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
        if (batch.lastResumeToken) {
            // mongocxx might give us a new resume token even if no change events are read.
            // Only update resumeTokenAdvancedSinceLastCheckpoint if the resume token is different
            // from the one we already have
            tassert(8017801, "Expected _state to have a startingPoint", _state.getStartingPoint());
            if (holds_alternative<BSONObj>(*_state.getStartingPoint())) {
                const auto& currentResumeToken = get<BSONObj>(*_state.getStartingPoint());
                if (!currentResumeToken.binaryEqual(*batch.lastResumeToken)) {
                    _resumeTokenAdvancedSinceLastCheckpoint = true;
                }
            }
            _state.setStartingPoint(
                std::variant<mongo::BSONObj, mongo::Timestamp>(std::move(*batch.lastResumeToken)));
        }
        if (_options.sendIdleMessages) {
            // If _options.sendIdleMessages is set, always send a kIdle watermark when
            // there are 0 docs in the batch.
            StreamControlMsg msg{
                .watermarkMsg = WatermarkControlMsg{.watermarkStatus = WatermarkStatus::kIdle}};
            _lastControlMsg = msg;
            sendControlMsg(0, std::move(msg));
        }
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

    incOperatorStats(OperatorStats{.numInputDocs = totalNumInputDocs,
                                   .numInputBytes = totalNumInputBytes,
                                   .numDlqDocs = numDlqDocs});

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

    if (_watermarkGenerator) {
        _stats.watermark = _watermarkGenerator->getWatermarkMsg().eventTimeWatermarkMs;
    }

    sendDataMsg(0, std::move(dataMsg), std::move(newControlMsg));
    tassert(7788508, "Expected resume token in batch", batch.lastResumeToken);
    _state.setStartingPoint(
        std::variant<mongo::BSONObj, mongo::Timestamp>(std::move(*batch.lastResumeToken)));
    _resumeTokenAdvancedSinceLastCheckpoint = true;
    LOGV2_DEBUG(7788507,
                2,
                "Change stream $source: updated resume token",
                "context"_attr = _context,
                "resumeToken"_attr = tojson(get<BSONObj>(*_state.getStartingPoint())));

    return totalNumInputDocs;
}

bool ChangeStreamSourceOperator::readSingleChangeEvent() {
    // TODO SERVER-77657: Handle invalidate events.
    invariant(_changeStreamCursor);

    boost::optional<mongo::BSONObj> changeEvent;
    boost::optional<mongo::BSONObj> eventResumeToken;

    // See if there are any available notifications. Note that '_changeStreamCursor->begin()'
    // will return the next available notification (that is, it will not reset our cursor to the
    // very beginning).
    if (_it == mongocxx::change_stream::iterator()) {
        _it = _changeStreamCursor->begin();
    }

    // If our cursor is exhausted, wait until the next call to 'readSingleChangeEvent' to try
    // reading from '_changeStreamCursor' again.
    if (_it != _changeStreamCursor->end()) {
        changeEvent = fromBsoncxxDocument(*_it);

        // Advance our cursor before processing the current document.
        ++_it;
    }

    // Get the latest resume token from the cursor. The resume token might advance
    // even if no documents are returned.
    auto resumeToken = _changeStreamCursor->get_resume_token();
    if (resumeToken) {
        eventResumeToken = fromBsoncxxDocument(std::move(*resumeToken));
    }
    tassert(8155200,
            "Expected resume token to be set whenever we read a change event.",
            resumeToken || !changeEvent);

    // If we've hit the end of our cursor, set our iterator to the default iterator so that we can
    // reset it on the next call to 'readSingleChangeEvent'.
    if (_it == _changeStreamCursor->end()) {
        _it = mongocxx::change_stream::iterator();
    }
    // Return early if we didn't read a change event or resume token.
    if (!changeEvent && !eventResumeToken) {
        return false;
    }

    {
        stdx::lock_guard<Latch> lock(_mutex);
        const auto capacity = _options.maxNumDocsToReturn;
        // Create a new vector if none exist or if the last vector is full.
        if (_changeEvents.empty() || int32_t(_changeEvents.back().size()) == capacity ||
            _changeEvents.back().getByteSize() >= kDataMsgMaxByteSize) {
            _memoryUsageHandle.set(_consumerStats.memoryUsageBytes);
            _changeEvents.emplace(capacity);
        }
        auto& activeBatch = _changeEvents.back();
        if (changeEvent) {
            int docSize = activeBatch.pushDoc(std::move(*changeEvent));
            _queueSizeGauge->incBy(1);
            _queueByteSizeGauge->incBy(docSize);
            _consumerStats += {.memoryUsageBytes = docSize};
        }
        if (eventResumeToken) {
            activeBatch.lastResumeToken = std::move(*eventResumeToken);
        }
    }

    return bool(changeEvent);
}

// Obtain the 'ts' field from either:
// - 'timeField' if the user specified
// - The 'wallTime' field (if we're reading from a change stream against a cluster whose
// version is GTE 6.0).
// - The 'clusterTime' (if we're reading from a change stream against a cluster whose
// version is LT 6.0).
//
// Then, does additional work to generate a watermark. Throws if a timestamp could not be obtained.
mongo::Date_t ChangeStreamSourceOperator::getTimestamp(const Document& changeEventDoc,
                                                       const Document& fullDocument) {
    mongo::Date_t ts;
    if (_options.timestampExtractor) {
        ts = _options.timestampExtractor->extractTimestamp(fullDocument);
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
        ts = Date_t::fromDurationSinceEpoch(Seconds{clusterTime.getTimestamp().getSecs()});
    }

    if (_watermarkGenerator) {
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
        if (_options.fullDocumentOnly) {
            // If fullDocumentOnly is set and is true, change stream event will always have a
            // fullDocument. We enforce this by failing the SP creation in case fullDocument mode is
            // not set to 'required' or 'updateLookup'
            ts = getTimestamp(
                changeEventDoc,
                changeEventDoc[DocumentSourceChangeStream::kFullDocumentField].getDocument());
            changeEventDoc = changeEventDoc[DocumentSourceChangeStream::kFullDocumentField]
                                 .getDocument()
                                 .getOwned();
        } else {
            ts = getTimestamp(changeEventDoc, changeEventDoc);
        }
    } catch (const DBException& e) {
        auto numDlqBytes = _context->dlq->addMessage(toDeadLetterQueueMsg(
            _context->streamMetaFieldName, std::move(changeEventDoc), e.toString()));
        incOperatorStats({.numDlqBytes = numDlqBytes});
        return boost::none;
    }

    MutableDocument mutableChangeEvent(std::move(changeEventDoc));

    // Add 'ts' to 'mutableChangeEvent', overwriting 'timestampOutputFieldName' if it already
    // exists.
    mutableChangeEvent[_options.timestampOutputFieldName] = Value(ts);
    StreamMeta streamMeta;
    streamMeta.setSourceType(StreamMetaSourceTypeEnum::Atlas);
    streamMeta.setTimestamp(ts);
    if (_context->shouldAddStreamMetaPriorToSinkStage()) {
        auto newStreamMeta = updateStreamMeta(
            mutableChangeEvent.peek().getField(*_context->streamMetaFieldName), streamMeta);
        mutableChangeEvent.setField(*_context->streamMetaFieldName,
                                    Value(std::move(newStreamMeta)));
    }

    StreamDocument streamDoc(mutableChangeEvent.freeze());
    streamDoc.streamMeta = std::move(streamMeta);

    streamDoc.minProcessingTimeMs = curTimeMillis64();
    streamDoc.minEventTimestampMs = ts.toMillisSinceEpoch();
    streamDoc.maxEventTimestampMs = ts.toMillisSinceEpoch();
    return streamDoc;
}

void ChangeStreamSourceOperator::initFromCheckpoint() {
    invariant(_restoreCheckpointState);
    _state = *_restoreCheckpointState;
    CHECKPOINT_RECOVERY_ASSERT(
        *_context->restoreCheckpointId,
        _operatorId,
        fmt::format("state has unexpected watermark: {}", bool(_state.getWatermark())),
        bool(_state.getWatermark()) == _options.useWatermarks);
    if (_options.useWatermarks) {
        // All watermarks start as active when restoring from a checkpoint.
        WatermarkControlMsg watermark{WatermarkStatus::kActive,
                                      _state.getWatermark()->getEventTimeMs()};
        _watermarkGenerator = std::make_unique<DelayedWatermarkGenerator>(0, /* inputIdx */
                                                                          nullptr /* combiner */,
                                                                          std::move(watermark));
    }
    LOGV2_INFO(7788505,
               "Change stream $source restored",
               "state"_attr = tojson(_state.toBSON()),
               "context"_attr = _context);
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

    if (_context->oldCheckpointStorage) {
        _context->oldCheckpointStorage->addState(checkpointId, _operatorId, _state.toBSON(), 0);
    } else {
        invariant(_context->checkpointStorage);
        auto writer = _context->checkpointStorage->createStateWriter(checkpointId, _operatorId);
        _context->checkpointStorage->appendRecord(writer.get(), Document{_state.toBSON()});
    }

    LOGV2_INFO(7788506,
               "Change stream $source: added state",
               "checkpointId"_attr = checkpointId,
               "context"_attr = _context,
               "state"_attr = tojson(_state.toBSON()));
    _uncommittedCheckpoints.push(std::make_pair(checkpointId, _state));
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
}

void ChangeStreamSourceOperator::doOnCheckpointCommit(CheckpointId checkpointId) {
    tassert(8444400, "Expected an uncommitted checkpoint", !_uncommittedCheckpoints.empty());
    // Checkpoints should always be committed in the order that they were added to
    // the `_uncommittedCheckpoints` queue.
    auto [id, checkpointState] = std::move(_uncommittedCheckpoints.front());
    _uncommittedCheckpoints.pop();
    tassert(8444401, "Unexpected checkpointId", id == checkpointId);
    _lastCommittedStartingPoint = checkpointState;
    _resumeTokenAdvancedSinceLastCheckpoint = false;
}

boost::optional<mongo::BSONObj> ChangeStreamSourceOperator::doGetRestoredState() {
    if (_restoreCheckpointState) {
        return _restoreCheckpointState->toBSON();
    }
    return boost::none;
}

boost::optional<mongo::BSONObj> ChangeStreamSourceOperator::doGetLastCommittedState() {
    if (_lastCommittedStartingPoint) {
        return _lastCommittedStartingPoint->toBSON();
    }
    return boost::none;
}

}  // namespace streams
