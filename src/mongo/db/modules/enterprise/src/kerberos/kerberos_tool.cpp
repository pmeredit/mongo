/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#include <iostream>

#include "kerberos_tool_options.h"

#include "mongo/base/initializer.h"
#include "mongo/logger/logger.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"

#if defined(_WIN32)
#include "mongo/util/text.h"
#endif

namespace mongo {

namespace {

int kerberosToolMain(int argc, char* argv[], char** envp) {
    setupSignalHandlers();
    runGlobalInitializersOrDie(argc, argv, envp);
    startSignalProcessingThread();

    if (globalKerberosToolOptions->verbose) {
        logger::globalLogDomain()->setMinimumLoggedSeverity(logger::LogSeverity::Debug(255));
    }

    std::cout << "Hello, world!" << std::endl;

    return 0;
}

}  // namespace
}  // namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through the argv() and envp() members.  This enables decryptToolMain()
// to process UTF-8 encoded arguments and environment variables without regard to platform.
int wmain(int argc, wchar_t* argvW[], wchar_t* envpW[]) {
    mongo::WindowsCommandLine wcl(argc, argvW, envpW);
    int exitCode = mongo::kerberosToolMain(argc, wcl.argv(), wcl.envp());
    mongo::quickExit(exitCode);
}
#else
int main(int argc, char* argv[], char** envp) {
    int exitCode = mongo::kerberosToolMain(argc, argv, envp);
    mongo::quickExit(exitCode);
}
#endif
