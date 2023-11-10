/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#pragma once

#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/pipeline/document_source_change_stream_gen.h"
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
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stages_gen.h"

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
        int64_t maxNumDocsToReturn{500};

        // Maximum number of documents this consumer should prefetch and have ready for the caller
        // to retrieve via getDocuments().
        // Note that we do not honor this limit strictly and we exceed this limit by at least
        // maxNumDocsToReturn depending on how many documents we wind up reading from our cursor.
        int64_t maxNumDocsToPrefetch{500 * 400};

        // The user-specified operation time to start at or resumeToken to startAfter.
        boost::optional<mongo::stdx::variant<mongo::BSONObj, mongo::Timestamp>>
            userSpecifiedStartingPoint;

        // Controls whether update changestream events contain the full document.
        mongo::FullDocumentModeEnum fullDocumentMode{mongo::FullDocumentModeEnum::kDefault};

        // If fullDocumentOnly is set to true, project the fullDocument and remove any metadata
        // from the change stream event.
        bool fullDocumentOnly{false};
    };


    ChangeStreamSourceOperator(Context* context, Options options);
    ~ChangeStreamSourceOperator();

    // Test only function to access configured options.
    const Options& getOptions() const {
        return _options;
    }

private:
    struct DocBatch {
        DocBatch(size_t capacity) {
            docs.reserve(capacity);
        }

        // Appends the given doc to docs.
        void pushDoc(mongo::BSONObj doc);

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

    void doStop() final;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

    std::string doGetName() const override {
        return "ChangeStreamConsumerOperator";
    }

    int64_t doRunOnce() final;

    void doConnect() override;

    ConnectionStatus doGetConnectionStatus() override;

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

    // Attempts to read a change event from '_changeEventCursor'. Returns true if a single event was
    // read and added to '_activeChangeEventDocBatch', false otherwise.
    bool readSingleChangeEvent();

    // Runs at the beginning of fetchLoop. Establishes a connection with the target and opens a
    // changestream.
    void connectToSource();

    Options _options;
    StreamControlMsg _lastControlMsg;

    // These fields must be set.
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri{nullptr};
    std::unique_ptr<mongocxx::client> _client{nullptr};
    std::unique_ptr<mongocxx::database> _database{nullptr};

    // This field may not be set.
    std::unique_ptr<mongocxx::collection> _collection{nullptr};

    // Thread responsible for reading change events from our cursor.
    mongo::stdx::thread _changeStreamThread;

    // Maintains the change stream cursor.
    std::unique_ptr<mongocxx::change_stream> _changeStreamCursor{nullptr};
    mongocxx::change_stream::iterator _it{mongocxx::change_stream::iterator()};

    // Options supplied to mongocxx. Configured in doOnStart.
    mongocxx::options::change_stream _changeStreamOptions;

    // State data that tracks our position in the $changestream. This is serialized and written
    // as OperatorState in checkpoint data.
    mongo::ChangeStreamSourceCheckpointState _state;

    // Watermark generator. Only set if watermarking is enabled.
    std::unique_ptr<DelayedWatermarkGenerator> _watermarkGenerator;

    // Guards the members below.
    mutable mongo::Mutex _mutex =
        MONGO_MAKE_LATCH("ChangeStreamSourceOperator::ChangeEvents::mutex");

    // Condition variable used by '_changeStreamThread'. Synchronized with '_mutex'.
    mongo::stdx::condition_variable _changeStreamThreadCond;

    // Queue of vectors of change events read from '_changeStreamCursor' that can be sent to the
    // rest of the OperatorDAG.
    std::queue<DocBatch> _changeEvents;

    // Tracks the total number of change events in '_changeEvents'.
    int64_t _numChangeEvents{0};

    // Tracks an exception that needs to be returned to the caller.
    std::exception_ptr _exception;

    // Whether '_changeStreamThread' should shut down. This is triggered when stop() is called or
    // an error is encountered.
    bool _shutdown{false};

    // ConnectionStatus of the changestream. Updated when connected succeeds and
    // whenever an error occurs.
    ConnectionStatus _connectionStatus;
};
}  // namespace streams
