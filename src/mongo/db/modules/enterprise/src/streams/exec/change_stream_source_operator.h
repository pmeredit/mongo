/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#pragma once

#include <mongocxx/change_stream.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "streams/exec/delayed_watermark_generator.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"
#include "streams/exec/stages_gen.h"

namespace streams {
/**
 * This is a source operator for a change stream. It tails change events from a change stream
 * and feeds those documents to the OperatorDag.
 */
class ChangeStreamSourceOperator : public SourceOperator {
public:
    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions)
            : SourceOperator::Options(std::move(baseOptions)) {}

        Options() = default;

        // Must be set.
        mongo::ServiceContext* svcCtx{nullptr};
        // Must be set.
        std::string uri;
        // Must be set.
        std::unique_ptr<DelayedWatermarkGenerator> watermarkGenerator;

        // The maximum number of change events that can be returned in a single vector of results
        int32_t maxNumDocsToReturn{500};

        // Maximum number of documents this consumer should prefetch and have ready for the caller
        // to retrieve via getDocuments().
        // Note that we do not honor this limit strictly and we exceed this limit by at least
        // maxNumDocsToReturn depending on how many documents we wind up reading from our cursor.
        int32_t maxNumDocsToPrefetch{500 * 10};

        // Namespace to target the change stream against. May be empty.
        mongo::NamespaceString nss;

        // The options to configure a change stream cursor.
        mongocxx::options::change_stream changeStreamOptions;
    };


    ChangeStreamSourceOperator(Context* context, Options options);
    ~ChangeStreamSourceOperator();

    void doStart() final;
    void doStop() final;

    std::string doGetName() const override {
        return "ChangeStreamConsumerOperator";
    }

    // Test only function to access configured options.
    const Options& getOptions() const {
        return _options;
    }

protected:
    int32_t doRunOnce() final;

private:
    // Interface to get documents to send to the OperatorDAG.
    std::vector<mongo::BSONObj> getDocuments();

    // Utility to obtain a timestamp from 'changeEventObj'.
    boost::optional<mongo::Date_t> getTimestamp(const mongo::BSONObj& changeEventObj);

    // Utility to convert 'changeStreamObj' into a StreamDocument.
    boost::optional<StreamDocument> processChangeEvent(mongo::BSONObj changeStreamObj);

    // '_consumerThread' uses this to continuously tail documents from '_changeStreamCursor'.
    void fetchLoop();

    // Attempts to read a change event from '_changeEventCursor'. Returns true if a single event was
    // read and added to '_activeChangeEventBatch', false otherwise.
    bool readSingleChangeEvent();

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

    // Guards the members below.
    mutable mongo::Mutex _mutex =
        MONGO_MAKE_LATCH("ChangeStreamSourceOperator::ChangeEvents::mutex");

    // Condition variable used by '_changeStreamThread'. Synchronized with '_mutex'.
    mongo::stdx::condition_variable _changeStreamThreadCond;

    // Queue of vectors of change events read from '_changeStreamCursor' that can be sent to the
    // rest of the OperatorDAG.
    std::queue<std::vector<mongo::BSONObj>> _changeEvents;

    // Tracks the total number of change events in '_changeEvents'.
    int32_t _numChangeEvents{0};

    // Tracks an exception that needs to be returned to the caller.
    std::exception_ptr _exception;

    // Whether '_changeStreamThread' should shut down. This is triggered when stop() is called or
    // an error is encountered.
    bool _shutdown{false};
};
}  // namespace streams
