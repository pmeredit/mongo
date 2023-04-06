#pragma once

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/pipeline.h"
#include "streams/exec/connection_gen.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/event_deserializer.h"
#include "streams/exec/kafka_consumer_operator.h"
#include "streams/exec/operator.h"
#include "streams/exec/operator_dag.h"
#include "streams/exec/operator_factory.h"
#include <memory>

using namespace mongo::literals;

namespace streams {

/**
 * Parser is the main entrypoint for the "frontend" of streams.
 * It takes user-provided pipeline BSON and converts it into an OperatorDag.
 * It's a small wrapper around the existing Pipeline parse and optimize mechanics,
 * plus an OperatorFactory to convert DocumentSource instances to streaming Operators.
 * A separate instance of Parser should be used per stream processor.
 */
class Parser {
public:
    static constexpr mongo::StringData kSourceStageName = "$source"_sd;
    static constexpr mongo::StringData kEmitStageName = "$emit"_sd;
    static constexpr mongo::StringData kMergeStageName = "$merge"_sd;
    static constexpr mongo::StringData kDefaultTsFieldName = "_ts"_sd;
    static constexpr mongo::StringData kDefaultTimestampOutputFieldName = "_ts"_sd;

    Parser(const std::vector<mongo::Connection>& connections);

    std::unique_ptr<OperatorDag> fromBson(const std::string& name,
                                          const std::vector<mongo::BSONObj>& bsonPipeline);

private:
    OperatorFactory _operatorFactory;
    mongo::stdx::unordered_map<std::string, mongo::Connection> _connectionObjs;
};

};  // namespace streams
