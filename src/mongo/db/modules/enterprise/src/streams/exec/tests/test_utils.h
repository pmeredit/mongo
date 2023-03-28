#include "mongo/bson/bsonobj.h"

namespace streams {

class TestUtils {
public:
    static mongo::BSONObj getTestLogSinkSpec();
    static mongo::BSONObj getTestMemorySinkSpec();
    static mongo::BSONObj getTestSourceSpec();
};

}  // namespace streams
