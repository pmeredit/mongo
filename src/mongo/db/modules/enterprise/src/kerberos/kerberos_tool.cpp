/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <cstdlib>

#include "kerberos_tool_options.h"

#include "mongo/base/initializer.h"
#include "mongo/logger/logger.h"
#include "mongo/util/net/hostname_canonicalization.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"

#include "sasl/mongo_gssapi.h"
#include "util/report.h"

#if defined(_WIN32)
#include "mongo/util/text.h"
#endif

namespace mongo {

namespace {

std::string getEnvironmentValue(StringData variable) {
    char* value;
    value = getenv(variable.rawData());
    if (value == nullptr) {
        return "not set.";
    } else {
        return value;
    }
}

enum class OutputType { kImportant, kSolution, kHighlight };
std::string formatOutput(StringData value, OutputType outputType) {
    constexpr StringData csiSequence = "\x1b["_sd;
    constexpr StringData ansiBoldBlue = "34;1m"_sd;
    constexpr StringData ansiNone = "\x1b[0m"_sd;
    constexpr StringData ansiYellow = "33m"_sd;
    constexpr StringData ansiBold = "1m"_sd;

    if (!globalKerberosToolOptions->color) {
        return value.toString();
    }

    switch (outputType) {
        case OutputType::kImportant:
            return str::stream() << csiSequence << ansiBoldBlue << value << ansiNone;
        case OutputType::kSolution:
            return str::stream() << csiSequence << ansiYellow << value << ansiNone;
        case OutputType::kHighlight:
            return str::stream() << csiSequence << ansiBold << value << ansiNone;
        default:
            return value.toString();
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
        if (result != 0) {
            std::cout << "Could not set environment variable KRB5_TRACE to stdout" << std::endl;
        }
    }

    report.openSection(str::stream()
                       << "Getting MIT Kerberos KRB5 "
                       << formatOutput("environment variables", OutputType::kHighlight));
    std::map<std::string, std::string> environment{{"KRB5CCNAME", ""},
                                                   {"KRB5_KTNAME", ""},
                                                   {"KRB5_CONFIG", ""},
                                                   {"KRB5_TRACE", ""},
                                                   {"KRB5_CLIENT_KTNAME", ""}};
    for (const auto& keyValuePair : environment) {
        environment.insert_or_assign(keyValuePair.first, getEnvironmentValue(keyValuePair.first));
    }

    report.printItemList([&] {
        std::vector<std::string> items;
        items.reserve(environment.size());
        for (const auto& kvPair : environment) {
            items.emplace_back(str::stream() << formatOutput(kvPair.first, OutputType::kImportant)
                                             << ": " << kvPair.second);
        }
        return items;
    });
    report.closeSection("");
#else
    }
#endif

    // check that the DNS name resolves to an IP address
    const std::string& kerbHost = globalKerberosToolOptions->gssapiHostName;
    report.openSection(
        str::stream() << "Verifying "
                      << formatOutput("forward and reverse DNS resolution ", OutputType::kHighlight)
                      << "works with Kerberos service "
                      << formatOutput(kerbHost, OutputType::kImportant));
    std::vector<std::string> kerberosFQDNs =
        getHostFQDNs(kerbHost, HostnameCanonicalizationMode::kForward);
    report.checkAssert({[&kerberosFQDNs] { return !kerberosFQDNs.empty(); },
                        str::stream()
                            << "Hostname " << formatOutput(kerbHost, OutputType::kImportant)
                            << formatOutput(" could not be canonicalized.", OutputType::kHighlight),
                        {formatOutput("Consider updating your DNS records", OutputType::kSolution),
                         formatOutput("Consider disabling hostname canonicalization in your "
                                      "krb5.conf with flag canonicalize=false.",
                                      OutputType::kSolution)},
                        Report::FailType::kNonFatalFailure});
    std::transform(kerberosFQDNs.begin(),
                   kerberosFQDNs.end(),
                   kerberosFQDNs.begin(),
                   [&](std::string& addr) -> std::string {
                       return formatOutput(addr, OutputType::kImportant);
                   });
    std::cout << "Performing " << formatOutput("reverse DNS lookup ", OutputType::kHighlight)
              << "of the following FQDNs:" << std::endl;
    report.printItemList([&kerberosFQDNs] { return kerberosFQDNs; });
    kerberosFQDNs = getHostFQDNs(kerbHost, HostnameCanonicalizationMode::kForwardAndReverse);
    report.checkAssert(
        {[&kerberosFQDNs] { return !kerberosFQDNs.empty(); },
         str::stream() << "Hostname " << formatOutput(kerbHost, OutputType::kImportant)
                       << " resolves to IP address, but "
                       << formatOutput(
                              "could not find hostname in reverse lookup of host's IP address.",
                              OutputType::kHighlight),
         {formatOutput(
             "Please update your DNS records so that the PTR records for the host's IP address "
             "refer back to the same host's FQDN. \n\tIf reverse dns checks are disabled in "
             "krb5.conf (i.e. rdns=false), then please disregard this warning.",
             OutputType::kSolution)},
         Report::FailType::kNonFatalFailure});
    report.closeSection(
        "Each IP address to which host resolves can be reverse-associated back to a valid "
        "hostname");

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
