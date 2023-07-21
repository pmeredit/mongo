#pragma once

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/mongocxx_utils.h"

namespace streams {

struct Context;

/**
 * MongoDBDeadLetterQueue implements the DeadLetterQueue interface
 * using a remote MongoDB collection. The mongocxx driver is used to
 * connect to the remote MongoDB.
 */
class MongoDBDeadLetterQueue : public DeadLetterQueue {
public:
    MongoDBDeadLetterQueue(Context* context, MongoCxxClientOptions options);

private:
    void doAddMessage(mongo::BSONObj msg) override;

    const MongoCxxClientOptions _options;
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri;
    std::unique_ptr<mongocxx::client> _client;
    std::unique_ptr<mongocxx::database> _database;
    std::unique_ptr<mongocxx::collection> _collection;
    mongocxx::options::insert _insertOptions;
};

}  // namespace streams
