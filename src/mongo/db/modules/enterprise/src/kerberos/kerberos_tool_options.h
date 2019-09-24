/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#pragma once

#include <string>

namespace mongo {

struct KerberosToolOptions {
    bool verbose = false;
    enum ConnectionType { kClient = 0, kServer = 1 };
    ConnectionType connectionType;
    std::string host;
    std::string username;
    std::string gssapiServiceName;
    std::string gssapiHostName;
};

extern KerberosToolOptions* globalKerberosToolOptions;

}  // namespace mongo
