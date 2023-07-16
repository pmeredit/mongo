#pragma once

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/instance.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"

namespace mongo {
class ServiceContext;
}

namespace streams {

// There should only be 1 mongocxx::instance object per process.
mongocxx::instance* getMongocxxInstance(mongo::ServiceContext* svcCtx);

bsoncxx::document::value toBsoncxxDocument(const mongo::BSONObj& obj);

template <class T>
mongo::BSONObj fromBsonCxxDocument(T value) {
    return mongo::fromjson(bsoncxx::to_json(std::move(value)));
}

}  // namespace streams
