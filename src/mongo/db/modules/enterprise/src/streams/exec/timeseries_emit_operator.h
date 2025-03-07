/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <string>

#include "mongo/db/pipeline/expression.h"
#include "mongo/db/timeseries/timeseries_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/queued_sink_operator.h"

namespace streams {

struct Context;
class MetricManager;

/**
 * The operator for $emit to Time Series collection.
 */
class TimeseriesEmitOperator : public QueuedSinkOperator {
public:
    struct Options {
        MongoCxxClientOptions clientOptions;
        mongo::TimeseriesSinkOptions timeseriesSinkOptions;
        boost::intrusive_ptr<mongo::Expression> dbExpr;
        boost::intrusive_ptr<mongo::Expression> collExpr;
    };

    TimeseriesEmitOperator(Context* context, Options options)
        : QueuedSinkOperator(context, 1 /* numInputs */, 1 /* parallelism */),
          _options(std::move(options)) {}

    std::unique_ptr<SinkWriter> makeWriter() override;

    std::string doGetName() const override {
        return "TimeseriesEmitOperator";
    }

    mongo::ConnectionTypeEnum getConnectionType() const override {
        return mongo::ConnectionTypeEnum::Atlas;
    }

private:
    Options _options;
};

/*
 * TimeseriesWriter handles actually inserting data into a timeseries collection.
 */
class TimeseriesWriter : public SinkWriter {
public:
    TimeseriesWriter(Context* context,
                     SinkOperator* sinkOperator,
                     TimeseriesEmitOperator::Options options);

protected:
    OperatorStats processDataMsg(StreamDataMsg dataMsg) override;

    void connect() override;

    void processDbAndCollExpressions(const StreamDocument& streamDoc);

private:
    OperatorStats processStreamDocs(StreamDataMsg dataMsg, size_t startIdx, size_t maxDocCount);

    // Queries the database to retrieve the TimeseriesOptions for the timeseries collection.
    boost::optional<mongo::TimeseriesOptions> getTimeseriesOptionsFromDb();

    TimeseriesEmitOperator::Options _options;
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri;
    std::unique_ptr<mongocxx::client> _client;
    std::unique_ptr<mongocxx::database> _database;
    std::unique_ptr<mongocxx::collection> _collection;
    mongocxx::options::insert _insertOptions;
    std::string _errorPrefix;
};

}  // namespace streams
