/**
 *  Copyright (C) 2019 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include <fstream>
#include <vector>

#include "kerberos_tool.h"
#include "kerberos_tool_options.h"

#include "mongo/base/initializer.h"
#include "mongo/logger/logger.h"
#include "mongo/util/log.h"
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
    // check that the DNS name resolves to an IP address
    const std::string& kerbHost = globalKerberosToolOptions->gssapiHostName.empty()
        ? globalKerberosToolOptions->host
        : globalKerberosToolOptions->gssapiHostName;
    report.openSection(
        str::stream() << "Verifying "
                      << formatOutput("forward and reverse DNS resolution ", OutputType::kHighlight)
                      << "works with Kerberos service "
                      << formatOutput(kerbHost, OutputType::kImportant));
    std::vector<std::string> serviceFQDNs =
        getHostFQDNs(kerbHost, HostnameCanonicalizationMode::kForward);
    report.checkAssert({[&serviceFQDNs] { return !serviceFQDNs.empty(); },
                        str::stream()
                            << "Hostname " << formatOutput(kerbHost, OutputType::kImportant)
                            << formatOutput(" could not be canonicalized.", OutputType::kHighlight),
                        {formatOutput("Consider updating your DNS records", OutputType::kSolution),
                         formatOutput("Consider disabling hostname canonicalization in your "
                                      "krb5.conf with flag canonicalize=false.",
                                      OutputType::kSolution)},
                        Report::FailType::kNonFatalFailure});
    std::cout << "Performing " << formatOutput("reverse DNS lookup ", OutputType::kHighlight)
              << "of the following FQDNs:" << std::endl;
    report.printItemList([&serviceFQDNs] {
        std::vector<std::string> formattedFQDNs;
        std::transform(serviceFQDNs.begin(),
                       serviceFQDNs.end(),
                       std::back_inserter(formattedFQDNs),
                       [&](std::string& addr) -> std::string {
                           return formatOutput(addr, OutputType::kImportant);
                       });
        return formattedFQDNs;
    });
    std::vector<std::string> serviceFQDNsReverse =
        getHostFQDNs(kerbHost, HostnameCanonicalizationMode::kForwardAndReverse);
    report.checkAssert(
        {[&serviceFQDNsReverse] { return !serviceFQDNsReverse.empty(); },
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

    // set up variables for kerberos API
    report.openSection("Resolving kerberos environment");
    const std::string& serviceName = globalKerberosToolOptions->gssapiServiceName;
    KerberosEnvironment krb5Env(globalKerberosToolOptions->connectionType);

    if (globalKerberosToolOptions->debug) {
        logger::globalLogDomain()->setMinimumLoggedSeverity(logger::LogSeverity::Debug(255));
#ifndef _WIN32
        int result = setenv("KRB5_TRACE", "/dev/stdout", 1);
        if (result != 0) {
            std::cout << "Could not set environment variable KRB5_TRACE to stdout" << std::endl;
        }
#endif
    }

#ifndef _WIN32
    report.openSection(str::stream()
                       << "Getting MIT Kerberos KRB5 "
                       << formatOutput("environment variables", OutputType::kHighlight));
    report.printItemList([&] {
        std::vector<std::string> items;
        items.reserve(krb5Env.variables().size());
        for (const auto& kvPair : krb5Env.variables()) {
            items.emplace_back(str::stream()
                               << formatOutput(kvPair.first, OutputType::kImportant) << ": "
                               << (kvPair.second.empty() ? "not set." : kvPair.second));
        }
        return items;
    });
    report.closeSection("");

    // check that KRB5 keytab exists
    report.openSection(str::stream()
                       << "Verifying " << formatOutput("existence", OutputType::kHighlight)
                       << " of KRB5 "
                       << (krb5Env.isClientConnection()
                               ? formatOutput("client ", OutputType::kHighlight)
                               : "")
                       << "keytab "
                       << formatOutput(krb5Env.getKeytabName().value_or("<keytab doesn't exist>"),
                                       OutputType::kImportant));
    if (krb5Env.isServerConnection()) {
        report.checkAssert({[&] { return krb5Env.keytabExistsAndIsPopulated(); },
                            "KRB5 keytab either doesn't exist or contains no entries."});
        report.closeSection("KRB5 keytab exists and is populated.");
    } else {
#ifdef MONGO_KRB5_HAVE_FEATURES
        report.checkAssert(
            {[&] { return krb5Env.keytabExistsAndIsPopulated(); },
             "KRB5 Client keytab does not exist.",
             {formatOutput("Users without a client keytab need to kinit in order to obtain TGTs.",
                           OutputType::kSolution)},
             Report::FailType::kNonFatalInfo});
        report.closeSection("KRB5 client keytab exists and is populated.");
#else
        report.checkAssert(
            {[&] { return !krb5Env.getKeytabName(); },
             "User has specified client keytab on a system that does not support them.",
             {"Your client keytab will not be used when attempting to authenticate to Kerberos.",
              formatOutput("Users on this system will need to kinit every time they wish to "
                           "authenticate to Kerberos.",
                           OutputType::kSolution),
              formatOutput("It is recommended to unset the KRB5_CLIENT_KTNAME environment variable "
                           "to avoid confusion.",
                           OutputType::kSolution)},
             Report::FailType::kNonFatalFailure});
        report.closeSection(
            "Kerberos does not understand client keytabs, and user has not specified one.");
#endif
    }

    report.closeSection("Kerberos environment resolved without errors.");

    // check that keytab has valid principal(s)
    report.openSection(str::stream()
                       << "Checking " << formatOutput("principal(s) ", OutputType::kHighlight)
                       << "in " << formatOutput("KRB5 keytab", OutputType::kHighlight));
    if (krb5Env.isClientConnection()) {
        // on client connection, check that at at least one principal listed in the client keytab or
        // the credentials cache has the same name as the one specified as --user
        const std::string& user = globalKerberosToolOptions->username;
        report.checkAssert({[&] {
                                return krb5Env.keytabContainsPrincipalWithName(serviceName) ||
                                    krb5Env.credentialsCacheContainsClientPrincipalName(user);
                            },
                            str::stream() << "Neither client keytab nor credentials cache contains "
                                             "entry with user principal name for specified --user "
                                          << formatOutput(user, OutputType::kImportant) << '.'});
    } else {
        // on server connection, check for users with specified gssapiServiceName and wrong DNS name
        std::vector<const KRB5KeytabEntry*> problemPrincipals;
        auto mongoDBEntries = krb5Env.keytabGetMongoDBEntries(serviceName);
        std::cout << "Found the following principals for MongoDB service "
                  << formatOutput(serviceName, OutputType::kImportant) << ":" << std::endl;
        report.printItemList([&]() {
            std::vector<std::string> parsedPrincipals;
            parsedPrincipals.reserve(mongoDBEntries.size());
            for (auto ktEntry : mongoDBEntries) {
                parsedPrincipals.emplace_back(ktEntry->getParsedPrincipal());
            }
            return parsedPrincipals;
        });
        for (const auto& entry : mongoDBEntries) {
            auto principalFQDNs = getHostFQDNs(entry->getPrincipalHost().toString(),
                                               HostnameCanonicalizationMode::kForward);
            if (!std::includes(serviceFQDNs.begin(),
                               serviceFQDNs.end(),
                               principalFQDNs.begin(),
                               principalFQDNs.end())) {
                problemPrincipals.emplace_back(entry);
            }
        }
        report.checkAssert(
            // assert that at least one host is valid
            {[&]() -> bool { return problemPrincipals.size() < mongoDBEntries.size(); },
             str::stream() << "Found "
                           << formatOutput("service principals ", OutputType::kHighlight)
                           << "in keytab with hosts that "
                           << formatOutput("do not resolve to same FQDN as provided service host",
                                           OutputType::kHighlight)
                           << ":",
             [&problemPrincipals]() -> std::vector<std::string> {
                 std::vector<std::string> formattedPrincipals;
                 for (const auto entry : problemPrincipals) {
                     formattedPrincipals.emplace_back(
                         formatOutput(entry->getParsedPrincipal(), OutputType::kSolution));
                 }
                 return formattedPrincipals;
             },
             Report::FailType::kFatalFailure});
    }
    report.closeSection("KRB5 keytab is valid.");

    report.openSection("Fetching KRB5 Config and log");
    std::string krb5ConfigContents;
    std::string ktraceLogContents;
    StringData krb5ConfigPath = krb5Env.getConfigPath();
    if (!krb5ConfigPath.empty()) {
        std::ifstream krb5Config(krb5ConfigPath.toString());
        krb5ConfigContents = std::string(std::istreambuf_iterator<char>(krb5Config),
                                         std::istreambuf_iterator<char>());
    }
    report.checkAssert({[&] { return !krb5ConfigContents.empty(); },
                        str::stream()
                            << "Deduced KRB5 Config location as " << krb5ConfigPath
                            << " but file at location either doesn't exist or is empty."});
    StringData ktraceLogPath = krb5Env.getTraceLocation();
    if (!ktraceLogPath.empty() && ktraceLogPath != "/dev/stdout") {
        std::ifstream ktraceLog(ktraceLogPath.toString());
        ktraceLogContents = std::string(std::istreambuf_iterator<char>(ktraceLog),
                                        std::istreambuf_iterator<char>());
        if (ktraceLogContents.empty()) {
            ktraceLogContents = "<empty>";
        }
        std::cout << "Displaying contents of KRB5_TRACE file from '" << ktraceLogPath << "'\n"
                  << ktraceLogContents << std::endl;
    }
    report.closeSection("");

#endif
    return 0;
}

KerberosEnvironment::KerberosEnvironment(KerberosToolOptions::ConnectionType connectionType)
#ifdef _WIN32
    : _connectionType(connectionType) {
}
#else
    : _variables{}, _connectionType(connectionType) {

    for (const StringData& envVar :
         {kKRB5CCNAME, kKRB5_KTNAME, kKRB5_CONFIG, kKRB5_TRACE, kKRB5_CLIENT_KTNAME}) {
        _variables.insert_or_assign(envVar, getEnvironmentValue(envVar));
    }

    // set up KRB5 API stuff
    krb5_init_context(&_krb5Context);

    _keytab = std::make_unique<KRB5Keytab>(_krb5Context, _connectionType);

    _credentialsCache = std::make_unique<KRB5CredentialsCache>(_krb5Context);
}

KRB5Keytab::KRB5Keytab(krb5_context krb5Context, KerberosToolOptions::ConnectionType connectionType)
    : _krb5Context(krb5Context) {
    std::string ktResolutionError = str::stream() << "Could not resolve keytab.";
    // both kt_default functions will resolve the keytab specified by the appropriate environment
    // variable if it is set, else it will resolve the one at the default install location.
    if (connectionType == KerberosToolOptions::ConnectionType::kClient) {
#ifdef MONGO_KRB5_HAVE_FEATURES
        uassert(31331, ktResolutionError, krb5_kt_client_default(_krb5Context, &_krb5Keytab) == 0);
#else
        // client keytabs aren't available on older versions of KRB5
        _krb5Keytab = nullptr;
#endif
    } else {
        uassert(31330, ktResolutionError, krb5_kt_default(_krb5Context, &_krb5Keytab) == 0);
    }

    // populate keytab viewer
    krb5_error_code errorCode;
    if (existsAndIsPopulated()) {
        krb5_keytab_entry currentEntry;
        krb5_kt_cursor ktCursor;
        auto endSeq = makeGuard([&]() {
            if (ktCursor != nullptr) {
                if (krb5_kt_end_seq_get(_krb5Context, _krb5Keytab, &ktCursor) != 0) {
                    warning() << "Could not end keytab iteration sequence properly.";
                }
            }
        });
        errorCode = krb5_kt_start_seq_get(_krb5Context, _krb5Keytab, &ktCursor);
        uassert(31343, "Malformed keytab: could not begin iteration sequence.", errorCode == 0);
        while ((errorCode = krb5_kt_next_entry(
                    _krb5Context, _krb5Keytab, &currentEntry, &ktCursor)) != KRB5_KT_END) {
            uassert(31332, "Malformed keytab: could not iterate through entries.", errorCode == 0);
            _keytabEntries.emplace_back(_krb5Context, currentEntry);
        }
    }
}
#endif

}  // namespace
}  // namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through the argv() and envp() members.  This enables
// decryptToolMain() to process UTF-8 encoded arguments and environment variables without regard
// to platform.
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
