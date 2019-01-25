/**
 * Copyright (C) 2018 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include <iostream>

#include "mongo/util/quick_exit.h"
#include "mongo/util/text.h"

namespace mongo {

int CryptDMain(int argc, char** argv, char** envp) {

    std::cout << "Placeholder" << std::endl;

    return EXIT_SUCCESS;
}

}  // namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF-16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through the argv() and envp() members.  This enables CrytpDMain()
// to process UTF-8 encoded arguments and environment variables without regard to platform.
int wmain(int argc, wchar_t* argvW[], wchar_t* envpW[]) {
    mongo::WindowsCommandLine wcl(argc, argvW, envpW);
    int exitCode = mongo::CryptDMain(argc, wcl.argv(), wcl.envp());
    mongo::quickExit(exitCode);
}
#else
int main(int argc, char* argv[], char** envp) {
    int exitCode = mongo::CryptDMain(argc, argv, envp);
    mongo::quickExit(exitCode);
}
#endif
