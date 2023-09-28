#pragma once

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "streams/exec/stages_gen.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

/**
 * Struct containing options commonly used to configure a client using the mongocxx driver.
 */
struct MongoCxxClientOptions {
    MongoCxxClientOptions() = default;
    MongoCxxClientOptions(const mongo::AtlasConnectionOptions& atlasOptions);

    // Utility which produces options for the cxx driver.
    mongocxx::options::client toMongoCxxClientOptions() const;

    mongo::ServiceContext* svcCtx{nullptr};
    std::string uri;
    boost::optional<std::string> database;
    boost::optional<std::string> collection;
    std::string pemFile;
    std::string caFile;
};

// There should only be 1 mongocxx::instance object per process.
mongocxx::instance* getMongocxxInstance(mongo::ServiceContext* svcCtx);

bsoncxx::document::value toBsoncxxDocument(const mongo::BSONObj& obj);

// TODO(SERVER-81424): Current implementation is quite inefficient as we convert to json first.
template <class T>
mongo::BSONObj fromBsonCxxDocument(T value) {
    return mongo::fromjson(bsoncxx::to_json(std::move(value)));
}

}  // namespace streams
