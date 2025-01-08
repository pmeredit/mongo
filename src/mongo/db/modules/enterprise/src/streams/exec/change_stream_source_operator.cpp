/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/change_stream_source_operator.h"

#include "mongo/util/timer.h"
#include "streams/util/exception.h"
#include <boost/optional/optional.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <chrono>
#include <cstdint>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/pipeline.hpp>
#include <mutex>
#include <variant>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/json.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/document_source_change_stream_gen.h"
#include "mongo/db/pipeline/resume_token.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/connection_status.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/stream_processor_feature_flags.h"
#include "streams/exec/stream_stats.h"
#include "streams/exec/util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace std::chrono_literals;

namespace streams {

using namespace mongo;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

namespace {

// If enabled failpoint the changestream background thread will sleep for a second before
// connecting.
MONGO_FAIL_POINT_DEFINE(changestreamSourceSleepBeforeConnect);

// If enabled the executor thread will sleep for some time after processing a batch of fetched
// events
MONGO_FAIL_POINT_DEFINE(changestreamSlowEventProcessing);

// Name of the error code field name in the raw server error object.
static constexpr char kErrorCodeFieldName[] = "code";

// Arbitrary error codes for invalid changestream aggregation pipelines
static constexpr ErrorCodes::Error kEmptyPipelineErrorCode{40323};
static constexpr ErrorCodes::Error kUnrecognizedPipelineStageNameErrorCode{40324};

// Helper function to get the timestamp of the latest event in the oplog. This involves
// invoking a command on the server and so can be slow.
mongo::Timestamp getLatestOplogTime(mongocxx::database* database,
                                    mongocxx::client* client,
                                    bool shouldUseWatchToInitClusterChangestream) {
    if (!database && shouldUseWatchToInitClusterChangestream) {
        // When targetting a whole cluster change stream, use a client_session
        // and dummy watch call to get an initial clusterTime. Prior to this, we were
        // calling hello on the config database, which requires Atlas Admin privileges for Atlas
        // clusters.
        mongocxx::client_session session{client->start_session()};
        mongocxx::options::change_stream options;
        auto cursor = std::make_unique<mongocxx::change_stream>(client->watch(session, options));
        auto response = session.cluster_time();
        auto clusterTime = response.find("clusterTime");
        uassert(8748304,
                "Expected an clusterTime in the response. Is the change stream $source a replset?",
                clusterTime != response.end());
        uassert(8748305,
                "Expected a clusterTime timestamp field.",
                clusterTime->type() == bsoncxx::type::k_timestamp);
        auto timestamp = clusterTime->get_timestamp();
        return {timestamp.timestamp, timestamp.increment};
    }

    // Run the hello command to test the connection and retrieve the current operationTime.
    // A failure will throw an exception.
    bsoncxx::document::value helloResponse{bsoncxx::document::view()};
    if (database) {
        // TODO(SERVER-95515): Remove this block once shouldUseWatchToInitClusterChangestream
        // feature flag is removed.
        helloResponse = callHello(*database);
    } else {
        // Use the config database because the driver requires us to call run_command under a
        // particular DB.
        const std::string defaultDb{"config"};
        auto configDatabase = client->database(defaultDb);
        helloResponse = callHello(configDatabase);
    }

    auto operationTime = helloResponse.find("operationTime");
    // With Atlas sources, we don't expect to hit this.
    uassert(8748300,
            "Expected an operationTime in the response. Is the change stream $source a replset?",
            operationTime != helloResponse.end());
    uassert(8308700,
            "Expected an operationTime timestamp field.",
            operationTime->type() == bsoncxx::type::k_timestamp);
    auto timestamp = operationTime->get_timestamp();
    return {timestamp.timestamp, timestamp.increment};
}

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
    if (db && !db->empty()) {
        _database = std::make_unique<mongocxx::database>(_client->database(*db));
    }
    const auto& coll = _options.clientOptions.collection;
    if (coll && !coll->empty()) {
        // Enforced in planner.
        tassert(ErrorCodes::InternalError, "Expected a database to be set", _database);
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

    auto dbName = _options.clientOptions.database ? *_options.clientOptions.database : "";
    auto collName = _options.clientOptions.collection ? *_options.clientOptions.collection : "";
    _errorPrefix = fmt::format("Change stream $source {}.{} failed", dbName, collName);
    _stats.connectionType = ConnectionTypeEnum::Atlas;
}

ChangeStreamSourceOperator::~ChangeStreamSourceOperator() {
    // '_changeStreamThread' must not be running on shutdown.
    dassert(!_changeStreamThread.joinable());

    std::queue<DocBatch> emptyQueue;
    std::swap(_changeEvents, emptyQueue);

    // Report 0 memory usage to SourceBufferManager.
    _context->sourceBufferManager->allocPages(
        _sourceBufferHandle.get(), 0 /* curSize */, 0 /* numPages */);
}

mongo::Seconds ChangeStreamSourceOperator::getChangeStreamLag() const {
    if (!_latestResumeToken) {
        return mongo::Seconds{0};
    }

    const auto& streamPoint = *_latestResumeToken;
    auto opLogLatestTs = _changestreamOperationTime.load().count();

    unsigned delta = 0;

    if (const BSONObj* resumeToken = std::get_if<mongo::BSONObj>(&streamPoint)) {
        unsigned tokenSecs = ResumeToken::parse(*resumeToken).getClusterTime().getSecs();
        if (opLogLatestTs > tokenSecs) {
            delta = opLogLatestTs - tokenSecs;
        }
    } else if (const mongo::Timestamp* ts = std::get_if<mongo::Timestamp>(&streamPoint)) {
        if (opLogLatestTs > ts->getSecs()) {
            delta = opLogLatestTs - ts->getSecs();
        }
    } else {
        tasserted(9043601, "Unexpected resume token type");
    }
    return mongo::Seconds{delta};
}

OperatorStats ChangeStreamSourceOperator::doGetStats() {
    OperatorStats stats{_stats};
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        stats += _consumerStats;

        // Always expose memory usage from the memory usage handle so that
        // the sum of each operator's memory usage equals the stream processor's
        // memory aggregator memory usage.
        stats.setMemoryUsageBytes(_memoryUsageHandle.getCurrentMemoryUsageBytes());
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
    _numReadSingleChangeEvent = metricManager->registerCounter(
        "read_single_change_event_count",
        /* description */ "Number of times readSingleChangeEvent is called",
        /*labels*/ getDefaultMetricLabels(_context));
}

ChangeStreamSourceOperator::DocBatch ChangeStreamSourceOperator::getDocuments() {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    // Throw '_exception' to the caller if one was raised.
    if (_exception) {
        uasserted(8112601,
                  fmt::format(
                      "Error encountered while reading from change stream $source for db: "
                      "{} and collection: {}",
                      _options.clientOptions.database ? *_options.clientOptions.database : "",
                      _options.clientOptions.collection ? *_options.clientOptions.collection : ""));
    }

    ScopeGuard guard([&] {
        // Make sure to signal _changeStreamThreadCond on all exit paths.
        _changeStreamThreadCond.notify_all();
    });

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

    if (!_state.getStartingPoint()) {
        _state.setStartingPoint(std::variant<mongo::BSONObj, mongo::Timestamp>(
            getLatestOplogTime(_database.get(),
                               _client.get(),
                               shouldUseWatchToInitClusterChangestream(_context->featureFlags))));
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

    _clientSession.reset(new mongocxx::client_session{_client->start_session()});
    if (_collection) {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _collection->watch(*_clientSession, _pipeline, _changeStreamOptions));
    } else if (_database) {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _database->watch(*_clientSession, _pipeline, _changeStreamOptions));
    } else {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _client->watch(*_clientSession, _pipeline, _changeStreamOptions));
    }

    _it = mongocxx::change_stream::iterator();
}

void ChangeStreamSourceOperator::fetchLoop() {
    auto fetchFunc = [this]() {
        if (MONGO_unlikely(changestreamSourceSleepBeforeConnect.shouldFail())) {
            // Sleep for a bit to simulate a slightly longer connection time.
            // This helped us repro a timing issue during a concurrent start+stop
            // we saw in the actual service.
            sleepFor(Seconds{1});
        }

        // Establish the connection and start the changestream.
        connectToSource();
        bool enableDataFlow = getOptions().enableDataFlow;
        if (!enableDataFlow) {
            // If data flow is disabled, read a single change event.
            // This helps detect problems like ChangeStreamHistoryLost,
            // especially while validating a modify request.
            readSingleChangeEvent();
        }
        {
            stdx::unique_lock lock(_mutex);
            _connectionStatus = {ConnectionStatus::kConnected};
        }

        if (!enableDataFlow) {
            // If data flow is disabled, return.
            return;
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

                // Report current memory usage to SourceBufferManager and allocate one page of
                // memory from it.
                bool allocSuccess = _context->sourceBufferManager->allocPages(
                    _sourceBufferHandle.get(),
                    _consumerStats.memoryUsageBytes /* curSize */,
                    1 /* numPages */);
                if (!allocSuccess) {
                    LOGV2_DEBUG(7788501,
                                1,
                                "Change stream $source sleeping when bytesBuffered: "
                                "{bytesBuffered}",
                                "context"_attr = _context,
                                "bytesBuffered"_attr = _consumerStats.memoryUsageBytes);

                    _changeStreamThreadCond.wait(lock);
                    LOGV2_DEBUG(
                        7788502,
                        1,
                        "Change stream $source waking up when bytesBuffered: {bytesBuffered}",
                        "context"_attr = _context,
                        "bytesBuffered"_attr = _consumerStats.memoryUsageBytes);
                    continue;  // Retry SourceBufferManager::allocPages().
                }
            }

            // Get some change events from our change stream cursor.
            readSingleChangeEvent();
            _numReadSingleChangeEvent->increment(1);
        }
    };
    auto status = runMongocxxNoThrow(
        std::move(fetchFunc), _context, ErrorCodes::Error{8681500}, _errorPrefix, *_uri);

    // Translate a few other user errors specific to change stream $source.
    auto translateCode = [&](ErrorCodes::Error newCode) -> SPStatus {
        return SPStatus{Status{newCode, status.toString()}, status.unsafeReason()};
    };
    switch (int32_t statusCode = status.code(); statusCode) {
        case ErrorCodes::ChangeStreamHistoryLost:
            // We cannot resume from this point in the changestream.
            status = translateCode(ErrorCodes::StreamProcessorCannotResumeFromSource);
            break;
        case ErrorCodes::NoMatchingDocument:
            if (_options.fullDocumentMode == FullDocumentModeEnum::kRequired) {
                // If the user specifies fullDocumentMode==kRequired, the server will return
                // NoMatchingDocument if the event doesn't have the corresponding postImage in the
                // oplog.
                status = translateCode(ErrorCodes::StreamProcessorInvalidOptions);
            }
            break;
        case ErrorCodes::BSONObjectTooLarge:
            // Currently a pipeline like [$source: {db: test}, $merge: {into: {db: test}}] will
            // create an infinite loop. The loop creates a document that keeps getting larger.
            // Eventually the _changestream server_ will complain with a BSONObjectTooLarge error.
            status = translateCode(ErrorCodes::StreamProcessorSourceDocTooLarge);
            break;
        case ErrorCodes::Error{19}:
            // mongocxx throws this error code when the watch call fails to connect to the target.
            // This is one of the places where mongocxx error codes don't align with the server
            // error codes.
            status = translateCode(ErrorCodes::StreamProcessorAtlasConnectionError);
            break;
        case kEmptyPipelineErrorCode:
            // Caused by a source pipeline aggregation stage specification having non-singular
            // number of fields
        case kUnrecognizedPipelineStageNameErrorCode:
            // Caused by a source pipeline aggregation having a unrecognized stage
        case ErrorCodes::ChangeStreamFatalError:
            // Caused by a source pipeline aggregation attempting to modify an incoming doc's _id
            // field
        case ErrorCodes::CommandNotSupportedOnView:
            // Caused by a changestream source pointing to a timeseries collection
            status = translateCode(ErrorCodes::StreamProcessorInvalidOptions);
            break;
        default:
            break;
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
        invariant(_context->checkpointStorage);
        auto reader = _context->checkpointStorage->createStateReader(*_context->restoreCheckpointId,
                                                                     _operatorId);
        auto record = _context->checkpointStorage->getNextRecord(reader.get());
        CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                   _operatorId,
                                   "expected state for changestream $source",
                                   record);
        bsonState = record->toBson();
        CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                   _operatorId,
                                   "expected bson state for changestream $source",
                                   bsonState);
        _restoreCheckpointState = ChangeStreamSourceCheckpointState::parseOwned(
            IDLParserContext("ChangeStreamSourceOperator"), std::move(*bsonState));
    }

    invariant(_client);
    invariant(!_changeStreamThread.joinable());
    _changeStreamThread = stdx::thread([this]() {
        try {
            fetchLoop();
        } catch (const std::exception& e) {
            // Note: fetchLoop has its own error handling internally so we don't expect to get this
            // exception.
            LOGV2_WARNING(8681501,
                          "Unexpected std::exception in changestream $source fetch loop",
                          "context"_attr = _context,
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
    auto dataMsg = StreamDataMsg{.creationTimer = mongo::Timer{}};
    auto batch = getDocuments();
    auto& changeEvents = batch.docs;
    dassert(int32_t(changeEvents.size()) <= _options.maxNumDocsToReturn);

    // Regardless of whether we have new input documents, update latest resumeToken
    if (batch.lastResumeToken) {
        _latestResumeToken =
            std::variant<mongo::BSONObj, mongo::Timestamp>((*batch.lastResumeToken));
    }

    // Refresh flag value for staleness monitorPeriod
    boost::optional<mongo::Seconds> monitorPeriod =
        getChangestreamSourceStalenessMonitorPeriod(_context->featureFlags);
    if (monitorPeriod) {
        _stalenessMonitorPeriod.store(*monitorPeriod);
    }

    // Return if no documents are available at the moment.
    if (changeEvents.empty()) {
        if (batch.lastResumeToken) {
            // mongocxx might give us a new resume token even if no change events are read.
            // Only update resumeTokenAdvancedSinceLastCheckpoint if the resume token is
            // different from the one we already have
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
        if (_options.sendIdleMessages && _isIdle.load()) {
            // If _options.sendIdleMessages is set, send a kIdle watermark when
            // there are 0 docs in the batch and the background thread has set _isIdle.
            StreamControlMsg msg{
                .watermarkMsg = WatermarkControlMsg{.watermarkStatus = WatermarkStatus::kIdle}};
            _lastControlMsg = msg;
            sendControlMsg(0, std::move(msg));
        }
        return 0;
    }

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
                                   .numDlqDocs = numDlqDocs,
                                   .timeSpent = dataMsg.creationTimer->elapsed()});

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
        _stats.watermark = _watermarkGenerator->getWatermarkMsg().watermarkTimestampMs;
    }

    sendDataMsg(0, std::move(dataMsg), std::move(newControlMsg));
    if (MONGO_unlikely(changestreamSlowEventProcessing.shouldFail())) {
        sleepFor(Milliseconds{2500});
    }

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

    // If we've hit the end of our cursor, set our iterator to the default iterator so that we
    // can reset it on the next call to 'readSingleChangeEvent'.
    if (_it == _changeStreamCursor->end()) {
        _it = mongocxx::change_stream::iterator();
    }

    // Store latest operationTime regardless of whether we get a new resumeToken/event or not
    _changestreamOperationTime.store(mongo::Seconds{_clientSession->operation_time().timestamp});

    if (changeEvent) {
        _changestreamLastEventReceivedAt = Date_t::now();
    }

    // Check for staleness
    if (_stalenessMonitorPeriod.load() != mongo::Seconds::zero()) {
        // If we have not received anything for the configured period, do an additional
        // staleness check
        if (Date_t::now() - _changestreamLastEventReceivedAt >
            mongo::duration_cast<mongo::Milliseconds>(_stalenessMonitorPeriod.load())) {
            auto client = std::make_unique<mongocxx::client>(
                *_uri, _options.clientOptions.toMongoCxxClientOptions());

            std::unique_ptr<mongocxx::database> database;
            const auto& db = _options.clientOptions.database;
            if (db && !db->empty()) {
                database = std::make_unique<mongocxx::database>(_client->database(*db));
            }

            auto currOplogTime = getLatestOplogTime(database.get(), client.get(), false);
            if (mongo::Seconds{currOplogTime.getSecs()} >
                _changestreamOperationTime.load() + mongo::Seconds{60}) {
                LOGV2_WARNING(
                    9588802, "change stream $source is possibly stale.", "context"_attr = _context);
                uasserted(ErrorCodes::ChangeStreamFatalError, "ChangeStream is possibly stale");
            }
        }
    }

    // Return early if we didn't read a change event or resume token.
    if (!changeEvent && !eventResumeToken) {
        _isIdle.store(true);
        return false;
    }

    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
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
            // We saw a change event, so mark isIdle false.
            _isIdle.store(false);
        } else {
            // We did not see a change event, so mark isIdle as true.
            _isIdle.store(true);
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
// Then, does additional work to generate a watermark. Throws if a timestamp could not be
// obtained.
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

    // If an exception is thrown when trying to get a timestamp, DLQ 'changeEventDoc' and
    // return.
    try {
        if (_options.fullDocumentOnly) {
            // If fullDocumentOnly is set and is true, we only process change stream event with
            // a fullDocument field. Any other event is sent to the dlq.
            uassert(ErrorCodes::BadValue,
                    str::stream() << "Missing fullDocument field in change stream event",
                    changeEventDoc[DocumentSourceChangeStream::kFullDocumentField].getType() ==
                        BSONType::Object);
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
            _context->streamMetaFieldName, changeEventDoc, getName(), e.toString()));
        incOperatorStats({.numDlqBytes = numDlqBytes});
        return boost::none;
    }

    MutableDocument mutableChangeEvent(std::move(changeEventDoc));

    // Add 'ts' to 'mutableChangeEvent', overwriting 'timestampOutputFieldName' if it already
    // exists.
    mutableChangeEvent[_options.timestampOutputFieldName] = Value(ts);
    StreamMetaSource streamMetaSource;
    streamMetaSource.setType(StreamMetaSourceTypeEnum::Atlas);
    StreamMeta streamMeta;
    streamMeta.setSource(std::move(streamMetaSource));
    if (_context->shouldProjectStreamMetaPriorToSinkStage()) {
        auto newStreamMeta = updateStreamMeta(
            mutableChangeEvent.peek().getField(*_context->streamMetaFieldName), streamMeta);
        mutableChangeEvent.setField(*_context->streamMetaFieldName,
                                    Value(std::move(newStreamMeta)));
    }

    StreamDocument streamDoc(mutableChangeEvent.freeze());
    streamDoc.streamMeta = std::move(streamMeta);

    streamDoc.minProcessingTimeMs = curTimeMillis64();
    streamDoc.minDocTimestampMs = ts.toMillisSinceEpoch();
    streamDoc.maxDocTimestampMs = ts.toMillisSinceEpoch();
    return streamDoc;
}

void ChangeStreamSourceOperator::initFromCheckpoint() {
    invariant(_restoreCheckpointState);
    _state = *_restoreCheckpointState;
    if (_options.useWatermarks) {
        if (_state.getWatermark()) {
            // All watermarks start as active when restoring from a checkpoint.
            WatermarkControlMsg watermark{WatermarkStatus::kActive,
                                          _state.getWatermark()->getEventTimeMs()};
            _watermarkGenerator =
                std::make_unique<DelayedWatermarkGenerator>(0, /* inputIdx */
                                                            nullptr /* combiner */,
                                                            std::move(watermark));
        } else {
            // In case of modify stream processor, where a new window pipeline is added, the change
            // stream source state will not have a watermark set.
            CHECKPOINT_RECOVERY_ASSERT(*_context->restoreCheckpointId,
                                       _operatorId,
                                       "Missing watermark in the change stream source state",
                                       _context->isModifiedProcessor);
        }
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
            WatermarkState{_watermarkGenerator->getWatermarkMsg().watermarkTimestampMs});
    }

    invariant(_context->checkpointStorage);
    auto writer = _context->checkpointStorage->createStateWriter(checkpointId, _operatorId);
    _context->checkpointStorage->appendRecord(writer.get(), Document{_state.toBSON()});
    // Close the writer.
    writer.reset();

    auto bsonState = _state.toBSON();
    LOGV2_INFO(7788506,
               "Change stream $source: added state",
               "checkpointId"_attr = checkpointId,
               "context"_attr = _context,
               "state"_attr = bsonState);
    _unflushedStateContainer.add(checkpointId, std::move(bsonState));
    sendControlMsg(0 /* outputIdx */, std::move(controlMsg));
    _resumeTokenAdvancedSinceLastCheckpoint = false;
}

BSONObj ChangeStreamSourceOperator::doOnCheckpointFlush(CheckpointId checkpointId) {
    auto bsonState = _unflushedStateContainer.pop(checkpointId);
    _lastCommittedStartingPoint = ChangeStreamSourceCheckpointState::parseOwned(
        IDLParserContext{"ChangeStreamSourceOperator::doOnCheckpointFlush"}, std::move(bsonState));
    return _lastCommittedStartingPoint->toBSON();
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

boost::optional<std::variant<BSONObj, Timestamp>> ChangeStreamSourceOperator::getCurrentState()
    const {
    auto startingPoint = _state.getStartingPoint();
    if (!startingPoint) {
        return boost::none;
    }
    if (holds_alternative<BSONObj>(*startingPoint)) {
        auto bson = get<BSONObj>(*startingPoint);
        bson.makeOwned();
        return boost::make_optional(std::variant<BSONObj, Timestamp>(std::move(bson)));
    } else {
        tassert(9233400,
                "Expected timestamp startingPoint",
                holds_alternative<Timestamp>(*startingPoint));
        return boost::make_optional(
            std::variant<BSONObj, Timestamp>(get<Timestamp>(*startingPoint)));
    }
}

}  // namespace streams
