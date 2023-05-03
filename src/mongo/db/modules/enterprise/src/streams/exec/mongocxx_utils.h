#include "mongo/bson/bsonobj.h"
#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/instance.hpp>

namespace mongo {
class ServiceContext;
}

namespace streams {

// There should only be 1 mongocxx::instance object per process.
mongocxx::instance* getMongocxxInstance(mongo::ServiceContext* svcCtx);

bsoncxx::document::value toBsoncxxDocument(const mongo::BSONObj& obj);

}  // namespace streams
