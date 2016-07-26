/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/bson/oid.h"

namespace mongo {
namespace queryable {

/**
 * Bringing up a queryable restore MongoDB requires a BackupDB MongoURI and a `snapshotId`. This
 * saves the parsed values of what comes in on the command line.
 */
class Context {
public:
    Context(std::string apiUri, OID snapshotId)
        : _apiUri(std::move(apiUri)), _snapshotId(snapshotId) {}

    const std::string& apiUri() const {
        return _apiUri;
    }

    OID snapshotId() const {
        return _snapshotId;
    }

private:
    std::string _apiUri;
    OID _snapshotId;
};


}  // namespace queryable
}  // namespace mongo
