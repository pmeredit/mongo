/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include <boost/optional.hpp>
#include <string>
#include <vector>

#include "mongo/client/mongo_uri.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

namespace mongo {
namespace moe = mongo::optionenvironment;
namespace queryable {

class QueryableGlobalOptions {
public:
    Status add(moe::OptionSection* options);
    Status store(const moe::Environment& params, const std::vector<std::string>& args);

    boost::optional<std::string> getApiUri() {
        return _apiUri;
    }

    boost::optional<OID> getSnapshotId() {
        return _snapshotId;
    }

private:
    boost::optional<std::string> _apiUri;
    boost::optional<OID> _snapshotId;
};

extern QueryableGlobalOptions queryableGlobalOptions;

}  // namespace queryable
}  // namespace mongo
