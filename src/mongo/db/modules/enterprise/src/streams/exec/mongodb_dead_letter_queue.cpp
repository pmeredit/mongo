/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include <chrono>

#include "mongo/platform/basic.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/mongodb_dead_letter_queue.h"

namespace streams {

using namespace mongo;

MongoDBDeadLetterQueue::MongoDBDeadLetterQueue(Context* context, Options options)
    : DeadLetterQueue(context), _options(options) {
    _instance = getMongocxxInstance(_options.svcCtx);
    _uri = std::make_unique<mongocxx::uri>(_options.mongodbUri);
    _client = std::make_unique<mongocxx::client>(*_uri);
    _database = std::make_unique<mongocxx::database>(_client->database(_options.database));
    _collection =
        std::make_unique<mongocxx::collection>(_database->collection(_options.collection));

    mongocxx::write_concern writeConcern;
    writeConcern.journal(true);
    writeConcern.acknowledge_level(mongocxx::write_concern::level::k_majority);
    // TODO(SERVER-76564): Handle timeouts, adjust this value.
    writeConcern.majority(/*timeout*/ stdx::chrono::milliseconds(60 * 1000));
    _insertOptions = mongocxx::options::insert().write_concern(std::move(writeConcern));
}

void MongoDBDeadLetterQueue::doAddMessage(BSONObj msg) {
    // TODO(SERVER-76564): Add some batching logic in this class.
    auto result = _collection->insert_one(toBsoncxxDocument(msg), _insertOptions);
    // TODO(SERVER-76564): Handle errors writing to DLQ.
    dassert(result);
}

}  // namespace streams
