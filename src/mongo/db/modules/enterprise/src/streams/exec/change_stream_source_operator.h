/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source_change_stream_gen.h"
#include "streams/exec/util.h"
#include "streams/util/metrics.h"
#include <mongocxx/change_stream.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/change_stream.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/unflushed_state_container.h"

namespace streams {
/**
 * This is a source operator for a change stream. It tails change events from a change stream
 * and feeds those documents to the OperatorDag.
 * The actual changestream reads occur in a background producer thread.
 */
class ChangeStreamSourceOperator : public SourceOperator {
public:
    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions, MongoCxxClientOptions clientOptions)
            : SourceOperator::Options(std::move(baseOptions)),
              clientOptions(std::move(clientOptions)) {}

        Options() = default;

        // Must be set.
        MongoCxxClientOptions clientOptions;

        // The maximum number of change events that can be returned in a single vector of results
        int64_t maxNumDocsToReturn{kDataMsgMaxDocSize};

        // The user-specified operation time to start at or resumeToken to startAfter.
        boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>> userSpecifiedStartingPoint;

        // Controls whether update changestream events contain the full document.
        mongo::FullDocumentModeEnum fullDocumentMode{mongo::FullDocumentModeEnum::kDefault};

        // If fullDocumentOnly is set to true, project the fullDocument and remove any metadata
        // from the change stream event.
        bool fullDocumentOnly{false};

        // Controls whether update changestream events contain the full document before the change.
        mongo::FullDocumentBeforeChangeModeEnum fullDocumentBeforeChangeMode{
            mongo::FullDocumentBeforeChangeModeEnum::kOff};

        // The pipeline to pushdown for the change stream server to process.
        std::vector<mongo::BSONObj> pipeline;
    };

    const SourceOperator::Options& getOptions() const override {
        return _options;
    }

    ChangeStreamSourceOperator(Context* context, Options options);
    ~ChangeStreamSourceOperator() override;

    boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>> getCurrentState() const;

    bool hasUncheckpointedState() const {
        return _resumeTokenAdvancedSinceLastCheckpoint;
    }

    // The delta between the timestamp of the last event in the oplog and the timestamp in the last
    // obtained resume token from the change stream. This function is called once per loop by the
    // executor thread
    mongo::Seconds getChangeStreamLag() const;

private:
    struct DocBatch {
        DocBatch(size_t capacity) {
            docs.reserve(capacity);
        }

        // Appends the given doc to docs.
        // Returns the size of the doc.
        int pushDoc(mongo::BSONObj doc);

        int64_t size() const {
            return docs.size();
        }

        int64_t getByteSize() const;

        // Events from the changestream.
        std::vector<mongo::BSONObj> docs;
        // The resumeToken of the last event in the batch.
        boost::optional<mongo::BSONObj> lastResumeToken;
        // Tracks the total number of bytes in docs.
        int64_t byteSize{0};
    };

    void doStart() override;
    void doStop() override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    std::string doGetName() const override {
        return "ChangeStreamConsumerOperator";
    }

    int64_t doRunOnce() final;

    ConnectionStatus doGetConnectionStatus() override;
    boost::optional<mongo::BSONObj> doGetRestoredState() override;
    boost::optional<mongo::BSONObj> doGetLastCommittedState() override;
    mongo::BSONObj doOnCheckpointFlush(CheckpointId checkpointId) override;

    // Initializes the internal state from a checkpoint.
    void initFromCheckpoint();

    // Interface to get documents to send to the OperatorDAG.
    DocBatch getDocuments();

    // Utility to obtain a timestamp from 'changeEventObj'.
    mongo::Date_t getTimestamp(const mongo::Document& changeEventObj,
                               const mongo::Document& fullDocument);

    // Utility to convert 'changeStreamObj' into a StreamDocument.
    boost::optional<StreamDocument> processChangeEvent(mongo::BSONObj changeStreamObj);

    // This is the entrypoint for '_changeStreamThread'.
    // It established the connection with the $source and starts reading.
    void fetchLoop();

    // Attempts to read a change event from '_changeEventCursor'. Returns true if a single event
    // was read and added to '_activeChangeEventDocBatch', false otherwise.
    bool readSingleChangeEvent();

    // Runs at the beginning of fetchLoop. Establishes a connection with the target and opens a
    // changestream.
    void connectToSource();

    // Merges `_consumerStats` into `_stats` before returning.
    OperatorStats doGetStats() override;

    void registerMetrics(MetricManager* metricManager) override;

    Options _options;

    // These fields must be set.
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri{nullptr};
    std::unique_ptr<mongocxx::client> _client{nullptr};
    std::unique_ptr<mongocxx::client_session> _clientSession{nullptr};
    std::unique_ptr<mongocxx::database> _database{nullptr};

    // Prefix used in error messages.
    std::string _errorPrefix;

    // This field may not be set.
    std::unique_ptr<mongocxx::collection> _collection{nullptr};

    boost::optional<mongo::ChangeStreamSourceCheckpointState> _restoreCheckpointState;

    // Thread responsible for reading change events from our cursor.
    mongo::stdx::thread _changeStreamThread;

    // Maintains the change stream cursor.
    std::unique_ptr<mongocxx::change_stream> _changeStreamCursor{nullptr};
    mongocxx::change_stream::iterator _it{mongocxx::change_stream::iterator()};

    // Options supplied to mongocxx. Configured in doOnStart.
    mongocxx::options::change_stream _changeStreamOptions;

    // Pipeline to pushdown to change stream server.
    mongocxx::pipeline _pipeline;

    // State data that tracks our position in the $changestream. This is serialized and written
    // as OperatorState in checkpoint data.
    mongo::ChangeStreamSourceCheckpointState _state;

    // Watermark generator. Only set if watermarking is enabled.
    std::unique_ptr<DelayedWatermarkGenerator> _watermarkGenerator;

    // Guards the members below.
    mutable mongo::stdx::mutex _mutex;

    // Condition variable used by '_changeStreamThread'. Synchronized with '_mutex'.
    mongo::stdx::condition_variable _changeStreamThreadCond;

    // Queue of vectors of change events read from '_changeStreamCursor' that can be sent to the
    // rest of the OperatorDAG.
    std::queue<DocBatch> _changeEvents;

    // Set to true when the background thread gets an empty response from the changestream
    // server, and false when a change event is read from the server.
    mongo::Atomic<bool> _isIdle{false};

    // Tracks an exception that needs to be returned to the caller.
    std::exception_ptr _exception;

    // Whether '_changeStreamThread' should shut down. This is triggered when stop() is called
    // or an error is encountered.
    bool _shutdown{false};

    // ConnectionStatus of the changestream. Updated when connected succeeds and
    // whenever an error occurs.
    ConnectionStatus _connectionStatus;

    // Stats tracked by the consumer thread. Write and read access to these stats must be
    // protected by `_mutex`. This will be merged with the root level `_stats`
    // when `doGetStats()` is called. Protected by `_mutex`.
    OperatorStats _consumerStats;

    // Stores the starting point and watermark in the last committed checkpoint.
    boost::optional<mongo::ChangeStreamSourceCheckpointState> _lastCommittedStartingPoint;

    // Stores state in checkpoints that have not yet been flushed to remote stoarge.
    UnflushedStateContainer _unflushedStateContainer;

    // Note that in the future (SERVER-82562), if we change the meaning of a checkpoint commit
    // to mean that it has been uploaded to S3, we still need to ensure that the
    // _resumeTokenAdvancedSinceLastCheckpoint update happens after the take-a-checkpoint
    // control message has propagated through the DAG and before we resume further data
    // processing
    bool _resumeTokenAdvancedSinceLastCheckpoint{false};

    // This is the resume token corresponding to the point where we are in the change stream.
    boost::optional<std::variant<mongo::BSONObj, mongo::Timestamp>> _latestResumeToken;
    // This is the timestamp of the last event in the oplog in the server. This is updated
    // from the auxiliary event fetching thread each time we get a new event on the change stream.
    mongo::Atomic<mongo::Seconds> _changestreamOperationTime;
    mongo::Date_t _changestreamLastEventReceivedAt{mongo::Date_t::min()};

    // This value is set via a feature flag. If set, an additional periodic check is made
    // to check for change stream source staleness. It is updated from the executor thread
    // and used from the change stream background thread.
    mongo::Atomic<mongo::Seconds> _stalenessMonitorPeriod{mongo::Seconds::zero()};

    // Metrics that track the number of docs and bytes prefetched.
    std::shared_ptr<IntGauge> _queueSizeGauge;
    std::shared_ptr<IntGauge> _queueByteSizeGauge;
    // Metric that track the number of readSingleChangeEvent calls.
    std::shared_ptr<Counter> _numReadSingleChangeEvent;
};
}  // namespace streams
