#pragma once

#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

#include "mongo/bson/bsonobj.h"
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

/**
 * Converts a BSONObj to a bsoncxx::document::view which does not own the underlying BSON buffer. If
 * a 'bsoncxx::document::value' which owns the BSON buffer is needed, use toBsoncxxValue() instead.
 */
inline bsoncxx::document::view toBsoncxxView(const mongo::BSONObj& obj) {
    return bsoncxx::document::view(reinterpret_cast<const uint8_t*>(obj.objdata()), obj.objsize());
}

/**
 * Converts a BSONObj to a bsoncxx::document::value which owns the underlying BSON buffer. If a
 * 'bsoncxx::document::view' which does not own the BSON buffer is needed, use toBsoncxxView()
 * instead.
 */
inline bsoncxx::document::value toBsoncxxValue(const mongo::BSONObj& obj) {
    return bsoncxx::document::value(toBsoncxxView(obj));
}

template <class T>
requires std::is_convertible_v<T, bsoncxx::document::view> mongo::BSONObj fromBsoncxxDocument(
    T value) {
    bsoncxx::document::view view = value;
    return mongo::BSONObj(reinterpret_cast<const char*>(view.data())).getOwned();
}

}  // namespace streams
