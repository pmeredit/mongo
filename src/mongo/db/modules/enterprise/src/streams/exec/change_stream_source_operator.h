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

        // The maximum number of events that can be obtained from a change stream cursor in one call
        // to 'doRunOnce'.
        size_t maxNumDocsToReturn{500};

        // Namespace to target the change stream against. May be empty.
        mongo::NamespaceString nss;

        // The options to configure a change stream cursor.
        mongocxx::options::change_stream changeStreamOptions;
    };

    ChangeStreamSourceOperator(Options options);
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
    // Utility to obtain a timestamp from 'changeEventObj'.
    boost::optional<mongo::Date_t> getTimestamp(const mongo::BSONObj& changeEventObj);

    // Utility to convert 'changeStreamObj' into a StreamDocument.
    boost::optional<StreamDocument> processChangeEvent(mongo::BSONObj changeStreamObj);

    Options _options;
    StreamControlMsg _lastControlMsg;

    // These fields must be set.
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri{nullptr};
    std::unique_ptr<mongocxx::client> _client{nullptr};

    // These fields may not be set.
    std::unique_ptr<mongocxx::database> _database{nullptr};
    std::unique_ptr<mongocxx::collection> _collection{nullptr};

    // Maintains the change stream cursor.
    std::unique_ptr<mongocxx::change_stream> _changeStreamCursor{nullptr};
    mongocxx::change_stream::iterator _it{mongocxx::change_stream::iterator()};
};
}  // namespace streams
