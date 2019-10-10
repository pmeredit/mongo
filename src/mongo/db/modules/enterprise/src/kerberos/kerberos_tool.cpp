/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <cstdlib>

#include "kerberos_tool_options.h"

#include "mongo/base/initializer.h"
#include "mongo/logger/logger.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"

#include "util/report.h"

#if defined(_WIN32)
#include "mongo/util/text.h"
#endif

namespace mongo {

namespace {

std::string getEnvironmentValue(const std::string& variable) {
    char* value;
    value = getenv(variable.c_str());
    if (value == nullptr) {
        return "not set.";
    } else {
        return value;
    }
}

int kerberosToolMain(int argc, char* argv[], char** envp) {
    setupSignalHandlers();
    runGlobalInitializersOrDie(argc, argv, envp);
    startSignalProcessingThread();

    Report report(globalKerberosToolOptions->color);

    if (globalKerberosToolOptions->debug) {
        logger::globalLogDomain()->setMinimumLoggedSeverity(logger::LogSeverity::Debug(255));
#ifndef _WIN32
        int result = setenv("KRB5_TRACE", "/dev/stdout", 1);
        if (result == 0) {
            std::cout << "KRB5_TRACE=/dev/stdout" << std::endl;
        } else {
            std::cout << "Could not set environment variable KRB5_TRACE" << std::endl;
        }
        std::cout << std::endl;
    }

    report.openSection("Getting MIT Kerberos KRB5 environment variables");
    std::map<std::string, std::string> environment{{"KRB5CCNAME", ""},
                                                   {"KRB5_KTNAME", ""},
                                                   {"KRB5_CONFIG", ""},
                                                   {"KRB5_TRACE", ""},
                                                   {"KRB5_CLIENT_KTNAME", ""}};
    for (const auto& keyValuePair : environment) {
        environment.insert_or_assign(keyValuePair.first, getEnvironmentValue(keyValuePair.first));
    }
    report.printItemList([&environment] {
        const StringData ansiBlue = globalKerberosToolOptions->color ? "\x1b[34m" : "";
        const StringData ansiNone = globalKerberosToolOptions->color ? "\x1b[0m" : "";
        std::vector<std::string> items;
        items.reserve(environment.size());
        for (const auto& kvPair : environment) {
            items.emplace_back(str::stream()
                               << ansiBlue << kvPair.first << ansiNone << ": " << kvPair.second);
        }
        return items;
    });
    report.closeSection("");
#else
    }
#endif

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
