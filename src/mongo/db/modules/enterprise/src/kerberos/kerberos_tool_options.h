/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#pragma once

#include <string>

#if !defined(_WIN32)
#include <unistd.h>
#endif

namespace mongo {

struct KerberosToolOptions {
    bool debug = false;
#ifdef _WIN32
    bool color = false;
#else
    bool color = isatty(STDOUT_FILENO);
#endif
    enum ConnectionType { kClient = 0, kServer = 1 };
    ConnectionType connectionType;
    std::string host;
    std::string username;
    std::string gssapiServiceName;
    std::string gssapiHostName;
};

extern KerberosToolOptions* globalKerberosToolOptions;

}  // namespace mongo
