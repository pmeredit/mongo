#pragma once

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include "streams/exec/dead_letter_queue.h"

namespace streams {

struct Context;

/**
 * MongoDBDeadLetterQueue implements the DeadLetterQueue interface
 * using a remote MongoDB collection. The mongocxx driver is used to
 * connect to the remote MongoDB.
 */
class MongoDBDeadLetterQueue : public DeadLetterQueue {
public:
    struct Options {
        mongo::ServiceContext* svcCtx;
        std::string mongodbUri;
        std::string database;
        std::string collection;
    };

    MongoDBDeadLetterQueue(Context* context, Options options);

private:
    void doAddMessage(mongo::BSONObj msg) override;

    const Options _options;
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri;
    std::unique_ptr<mongocxx::client> _client;
    std::unique_ptr<mongocxx::database> _database;
    std::unique_ptr<mongocxx::collection> _collection;
    mongocxx::options::insert _insertOptions;
};

}  // namespace streams
