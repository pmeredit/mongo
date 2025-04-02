/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/exec/stages_gen.h"

namespace streams {

// Thread-safe container for storing and updating Connections
class ConnectionCollection {
public:
    ConnectionCollection();

    ConnectionCollection(std::vector<mongo::Connection> connections);

    void update(const mongo::Connection& connection);

    bool contains(const std::string& name);

    mongo::Connection at(const std::string& name);

private:
    static constexpr mongo::StringData kUnknownConnectionErrMsg{
        "No connection found with that name"};

    mongo::stdx::unordered_map<std::string, mongo::Connection> _nameToConnection;
    mongo::stdx::mutex _mutex;
};
}  // namespace streams
