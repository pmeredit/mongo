/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>

#include "mongo/bson/oid.h"

namespace mongo {
namespace queryable {

class QueryableGlobalOptions {
public:
    boost::optional<std::string> getApiUri() {
        return _apiUri;
    }

    boost::optional<OID> getSnapshotId() {
        return _snapshotId;
    }

    // WiredTiger config strings can take a list of `extensions`. This should return the extension
    // string for a custom filesystem for queryable_wt mode that can be combined with other
    // extensions such as encryption at rest. Example string to return:
    //
    // local={entry=queryableWtFsCreate,early_load=true,config={apiUri="127.0.0.1:8097",
    // snapshotId="57fd587b4c1e526d78349c2b",dbpath=/data/db/}}
    std::string getWiredTigerExtensionConfig(const std::string& dbpath);

    // Marshal configuration options into this class.
    Status store();

private:
    boost::optional<std::string> _apiUri;
    boost::optional<OID> _snapshotId;
};

extern QueryableGlobalOptions queryableGlobalOptions;

}  // namespace queryable
}  // namespace mongo
