/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/connection_collection.h"

#include <string>
#include <vector>

#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util.h"

namespace streams {

using namespace mongo;

ConnectionCollection::ConnectionCollection() {}

ConnectionCollection::ConnectionCollection(std::vector<Connection> connections) {
    for (auto& connection : connections) {
        uassert(ErrorCodes::InternalError,
                "Connection names must be unique",
                !_nameToConnection.contains(connection.getName().toString()));
        _nameToConnection.emplace(
            std::make_pair(connection.getName().toString(), std::move(connection)));
    }
}

void ConnectionCollection::update(const Connection& connection) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    const auto it = _nameToConnection.find(connection.getName().toString());

    uassert(ErrorCodes::InternalError, kUnknownConnectionErrMsg, it != _nameToConnection.end());

    uassert(ErrorCodes::InternalError,
            "Cannot update a connection with a different type",
            it->second.getType() == connection.getType());

    it->second = std::move(connection);
}

bool ConnectionCollection::contains(const std::string& name) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _nameToConnection.contains(name);
}

Connection ConnectionCollection::at(const std::string& name) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);

    const auto it = _nameToConnection.find(name);
    uassert(ErrorCodes::InternalError, kUnknownConnectionErrMsg, it != _nameToConnection.end());
    return it->second;
}

}  // namespace streams
