/**
 * Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_options.h"

#include "mongo/util/net/hostandport.h"
#include "search/search_index_options_gen.h"

namespace mongo {

SearchIndexParams globalSearchIndexParams;

Status SearchIndexParams::onValidateHost(StringData str, const boost::optional<TenantId>&) {
    // Unset value is OK
    if (str.empty()) {
        return Status::OK();
    }

    // `searchIndexAtlasHostAndPort` must be able to parse into a HostAndPort.
    if (auto status = HostAndPort::parse(str); !status.isOK()) {
        return status.getStatus().withContext(
            "searchIndexAtlasHostAndPort must be of the form \"host:port\"");
    }

    return Status::OK();
}

}  // namespace mongo
