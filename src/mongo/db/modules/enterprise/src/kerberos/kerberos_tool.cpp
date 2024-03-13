/**
 *  Copyright (C) 2019 MongoDB Inc.
 */


#include "mongo/platform/basic.h"

#include <cstdlib>
#include <fstream>
#include <vector>

#include "kerberos_tool.h"
#include "kerberos_tool_options.h"

#if !defined(_WIN32)
#include "util/gssapi_helpers.h"
#include <gssapi.h>
#include <gssapi/gssapi_krb5.h>
#endif

#include "mongo/base/initializer.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_component_settings.h"
#include "mongo/logv2/log_manager.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/util/net/hostname_canonicalization.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "util/report.h"

#if defined(_WIN32)
#include "mongo/util/text.h"
#include <windows.h>
#endif

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault


namespace mongo {

namespace {

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

/**
 *  Reads a GSSAPI/KRB5 status code and determines what advice should be given to the user
 *  in order to fix the problem. This advice is enqueued to adviceBullets. If no advice is
 *  available for the given status, this function returns an empty vector.
 */
#if !defined(_WIN32)
std::vector<std::string> getAdviceForStatus(OM_uint32 majorStatus,
                                            OM_uint32 minorStatus,
                                            KerberosToolOptions::ConnectionType connectionType) {
    bool isClientConnection = connectionType == KerberosToolOptions::ConnectionType::kClient;
    std::vector<std::string> adviceBullets;
    auto advise = [&adviceBullets](const std::string& s) {
        adviceBullets.emplace_back(formatOutput(s, OutputType::kSolution));
    };
    adviceBullets.emplace_back(getGssapiErrorString(majorStatus, minorStatus));
    signed long code =
        (minorStatus - static_cast<OM_uint32>(ERROR_TABLE_BASE_krb5)) + ERROR_TABLE_BASE_krb5;
    switch (code) {
        case KRB5_NO_TKT_SUPPLIED:
            if (isClientConnection) {
                advise("Try creating a new ticket for the desired user with kinit.");
            } else {
                advise("Try creating a new ticket for the desired service with kinit.");
            }
            break;
        case KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN:
            advise(
                "Make sure the KDC has a record for the desired service "
                "principal.");
            break;
        case KRB5_KDC_UNREACH:
            advise(
                "Ensure that the desired realm's KDC is running, and that "
                "this computer "
                "is able to ping the KDC.");
            break;
        case KRB5KRB_AP_ERR_SKEW:
            if (isClientConnection) {
                advise(
                    "Ensure the difference between client and KDC's clock is less than defined "
                    "clock skew (default 300 seconds).");
            } else {
                advise(
                    "Ensure the difference between service and KDC's clock is less than defined "
                    "clock skew (default 300 seconds).");
            }
            break;
        case KRB5KDC_ERR_ETYPE_NOSUPP:
            advise("Ensure the client is using a cipher supported by the KDC.");
            break;
        case KRB5KRB_AP_ERR_TKT_EXPIRED:
            advise("Try refreshing your credentials with kinit.");
            break;
        case KRB5KRB_AP_ERR_BAD_INTEGRITY:
            if (isClientConnection) {
                advise(
                    "Ensure that the current password being used by the client is known by the "
                    "KDC.");
            } else {
                advise(
                    "Ensure that the keytab file contains an up-to-date key for the service. Check "
                    "that its kvno matches that which is tracked by the KDC.");
                advise(
                    str::stream()
                    << "Please run the following command on the service host and verify that the "
                       "output kvno is the same as the latest kvno on the KDC:\n\t\t"
                    << "kvno -S " << globalKerberosToolOptions->gssapiServiceName << " "
                    << globalKerberosToolOptions->getGSSAPIHost());
            }

            break;
        default:
            break;
    }
    return adviceBullets;
}
#endif

#if !defined(_WIN32)
void performGSSAPIHandshake(Report& report,
                            KerberosToolOptions::ConnectionType connectionType,
                            std::string initiatorName) {
    OM_uint32 minorStatus;
    OM_uint32 majorStatus;
    OM_uint32 retFlags;
    OM_uint32 timeRec = GSS_C_INDEFINITE;
    GSSBufferDesc outputToken;
    GSSCredId initiatorCredHandle;
    GSSContextID contextHandle;
    std::string hostBasedService = globalKerberosToolOptions->getHostbasedService();
    gss_buffer_desc hostBasedServiceBuffer;
    hostBasedServiceBuffer.length = hostBasedService.size();
    hostBasedServiceBuffer.value = const_cast<char*>(hostBasedService.c_str());
    GSSName gssServiceName;
    std::vector<std::string> gssapiErrorBullets;
    auto errorBulletCallback = [&gssapiErrorBullets] { return gssapiErrorBullets; };

    report.checkAssert({[&] {
                            majorStatus = gss_import_name(&minorStatus,
                                                          &hostBasedServiceBuffer,
                                                          GSS_C_NT_HOSTBASED_SERVICE,
                                                          gssServiceName.get());
                            gssapiErrorBullets =
                                getAdviceForStatus(majorStatus,
                                                   minorStatus,
                                                   globalKerberosToolOptions->connectionType);
                            return majorStatus == GSS_S_COMPLETE;
                        },
                        "Could not import GSSAPI target name.",
                        errorBulletCallback,
                        Report::FailType::kFatalFailure});

    gss_buffer_desc initiatorNameBuffer;
    initiatorNameBuffer.length = initiatorName.size();
    initiatorNameBuffer.value = const_cast<char*>(initiatorName.c_str());
    GSSName gssInitiatorName;
    gss_OID nameType = connectionType == KerberosToolOptions::ConnectionType::kClient
        ? GSS_C_NT_USER_NAME
        : GSS_C_NT_HOSTBASED_SERVICE;
    report.checkAssert(
        {[&] {
             majorStatus = gss_import_name(
                 &minorStatus, &initiatorNameBuffer, nameType, gssInitiatorName.get());
             gssapiErrorBullets = getAdviceForStatus(
                 majorStatus, minorStatus, globalKerberosToolOptions->connectionType);
             return majorStatus == GSS_S_COMPLETE;
         },
         "Could not import GSSAPI user name.",
         errorBulletCallback,
         Report::FailType::kFatalFailure});

    report.checkAssert({[&] {
                            majorStatus = gss_acquire_cred(&minorStatus,
                                                           *gssInitiatorName.get(),
                                                           GSS_C_INDEFINITE,
                                                           GSS_C_NO_OID_SET,
                                                           GSS_C_INITIATE,
                                                           initiatorCredHandle.get(),
                                                           nullptr,
                                                           nullptr);
                            gssapiErrorBullets =
                                getAdviceForStatus(majorStatus,
                                                   minorStatus,
                                                   globalKerberosToolOptions->connectionType);
                            return majorStatus == GSS_S_COMPLETE;
                        },
                        "Could not acquire credentials.",
                        errorBulletCallback,
                        Report::FailType::kFatalFailure});

    report.checkAssert(
        {[&] {
             // mech_type has to be const_cast for older versions of GSSAPI in which
             // gss_mech_krb5 was * const
             majorStatus =
                 gss_init_sec_context(&minorStatus,                       /* minor_status */
                                      *initiatorCredHandle.get(),         /* claimant_cred_handle */
                                      contextHandle.get(),                /* context_handle */
                                      *gssServiceName.get(),              /* target_name */
                                      const_cast<gss_OID>(gss_mech_krb5), /* mech_type */
                                      GSS_C_INTEG_FLAG | GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG |
                                          GSS_C_CONF_FLAG, /* req_flags */
                                      false,               /* time_req */
                                      nullptr,             /* input_chan_bindings */
                                      GSS_C_NO_BUFFER,     /* input_token */
                                      nullptr,             /* actual_mech_type */
                                      outputToken.get(),   /* output_token */
                                      &retFlags,           /* ret_flags */
                                      &timeRec);           /* time_rec */

             if (GSS_ERROR(majorStatus) && !(majorStatus & GSS_S_CONTINUE_NEEDED)) {
                 gssapiErrorBullets = getAdviceForStatus(
                     majorStatus, minorStatus, globalKerberosToolOptions->connectionType);
                 // this will only be reached if there is a fatal error
                 return false;
             }
             return true;
         },
         "GSSAPI client handshake failed",
         errorBulletCallback,
         Report::FailType::kFatalFailure});
}
#else
#endif

int kerberosToolMain(int argc, char* argv[]) {
    setupSignalHandlers();
    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));
    startSignalProcessingThread();

    Report report(globalKerberosToolOptions->color);
    // set up variables for kerberos API
    report.openSection("Resolving kerberos environment");
    const std::string& serviceName = globalKerberosToolOptions->gssapiServiceName;
    StatusWith<KerberosEnvironment> swKrb5Env =
        KerberosEnvironment::create(globalKerberosToolOptions->connectionType);
    report.checkAssert({[&swKrb5Env]() { return swKrb5Env.isOK(); },
                        "Could not initialize Kerberos context.",
                        {swKrb5Env.getStatus().toString()},
                        Report::FailType::kFatalFailure});
    KerberosEnvironment krb5Env = std::move(swKrb5Env.getValue());
#ifndef _WIN32
    boost::optional<bool> rdnsState = krb5Env.getProfile().rdnsState();
#else
    boost::optional<bool> rdnsState = boost::none;
#endif

    report.closeSection("Kerberos environment resolved without errors.");

    // check that the DNS name resolves to an IP address
    const std::string& kerbHost = globalKerberosToolOptions->getGSSAPIHost();
    report.openSection(str::stream() << "Verifying "
                                     << formatOutput(rdnsState == boost::none || *rdnsState == true
                                                         ? "forward and reverse DNS resolution "
                                                         : "DNS resolution ",
                                                     OutputType::kHighlight)
                                     << "works with Kerberos service at "
                                     << formatOutput(kerbHost, OutputType::kImportant));
    auto swServiceFQDNs = getHostFQDNs(kerbHost, HostnameCanonicalizationMode::kForward);
    report.checkAssert(
        {[&swServiceFQDNs] { return swServiceFQDNs.isOK() && !swServiceFQDNs.getValue().empty(); },
         str::stream() << "Hostname " << formatOutput(kerbHost, OutputType::kImportant)
                       << formatOutput(" could not be canonicalized.", OutputType::kHighlight),
         {formatOutput("Consider updating your DNS records", OutputType::kSolution),
          formatOutput("Consider disabling hostname canonicalization in your "
                       "krb5.conf with flag canonicalize=false.",
                       OutputType::kSolution)},
         Report::FailType::kNonFatalFailure});
    auto serviceFQDNs = swServiceFQDNs.getValue();
    if (rdnsState == boost::none || *rdnsState == true) {
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

        auto swServiceFQDNsReverse =
            getHostFQDNs(kerbHost, HostnameCanonicalizationMode::kForwardAndReverse);
        report.checkAssert(
            {[&swServiceFQDNsReverse] {
                 return swServiceFQDNsReverse.isOK() && !swServiceFQDNsReverse.getValue().empty();
             },
             str::stream() << "Hostname " << formatOutput(kerbHost, OutputType::kImportant)
                           << " resolves to IP address, but "
                           << formatOutput(
                                  "could not find hostname in reverse lookup of host's IP address.",
                                  OutputType::kHighlight),
             {formatOutput(rdnsState.value_or(false) == true
                               ? "Please update your DNS records so that the PTR records for the "
                                 "host's IP address "
                                 "refer back to the same host's FQDN."
                               : "Consider disabling hostname canonicalization.",
                           OutputType::kSolution)},
             Report::FailType::kNonFatalFailure});
    }

    report.closeSection("DNS test successful.");

    if (globalKerberosToolOptions->debug) {
        logv2::LogManager::global().getGlobalSettings().setMinimumLoggedSeverity(
            logv2::LogComponent::kDefault,
            logv2::LogSeverity::Debug(logv2::LogSeverity::kMaxDebugLevel));
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

    // check that keytab has valid principal(s)
    report.openSection(str::stream()
                       << "Checking " << formatOutput("principal(s) ", OutputType::kHighlight)
                       << "in " << formatOutput("KRB5 keytab", OutputType::kHighlight));
    if (krb5Env.isClientConnection()) {
        // on client connection, check that at at least one principal listed in the client keytab or
        // the credentials cache has the same name as the one specified as --user
        const std::string& user = globalKerberosToolOptions->username;
        report.checkAssert({[&] {
                                return krb5Env.keytabContainsPrincipalWithName(user) ||
                                    krb5Env.credentialsCacheContainsClientPrincipalName(user);
                            },
                            str::stream() << "Neither client keytab nor credentials cache contains "
                                             "entry with user principal name for specified --user "
                                          << formatOutput(user, OutputType::kImportant) << '.'});
    } else {
        // on server connection, check for users with specified gssapiServiceName and wrong DNS name
        auto mongoDBEntries = krb5Env.keytabGetMongoDBEntries(serviceName);
        report.checkAssert({[&mongoDBEntries]() { return !mongoDBEntries.empty(); },
                            str::stream()
                                << "Keytab does not contain any entries for provided service "
                                << formatOutput(serviceName, OutputType::kImportant)});
        std::vector<const KRB5KeytabEntry*> problemPrincipals;
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
            auto swPrincipalFQDNs = getHostFQDNs(entry->getPrincipalHost().toString(),
                                                 HostnameCanonicalizationMode::kForward);
            if (swPrincipalFQDNs.isOK() &&
                !std::includes(serviceFQDNs.begin(),
                               serviceFQDNs.end(),
                               swPrincipalFQDNs.getValue().begin(),
                               swPrincipalFQDNs.getValue().end())) {
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

        std::cout << "Found the following " << formatOutput("kvnos", OutputType::kHighlight)
                  << " in keytab entries for service "
                  << formatOutput(serviceName, OutputType::kImportant) << std::endl;
        report.printItemList([&mongoDBEntries]() {
            std::vector<std::string> kvnos;
            std::transform(
                mongoDBEntries.begin(),
                mongoDBEntries.end(),
                std::back_inserter(kvnos),
                [](const KRB5KeytabEntry* entry) { return std::to_string(entry->getKvno()); });
            return kvnos;
        });
    }

    report.closeSection("KRB5 keytab is valid.");

    report.openSection("Fetching KRB5 Config");
    std::string krb5ConfigContents;
    std::string ktraceLogContents;
    StringData krb5ConfigPath = krb5Env.getConfigPath();
    if (!krb5ConfigPath.empty()) {
        std::ifstream krb5Config(krb5ConfigPath.toString());
        krb5ConfigContents = std::string(std::istreambuf_iterator<char>(krb5Config),
                                         std::istreambuf_iterator<char>());
        report.checkAssert({[&] { return !krb5ConfigContents.empty(); },
                            str::stream()
                                << "Deduced KRB5 Config location as " << krb5ConfigPath
                                << " but file at location either doesn't exist or is empty."});
    }
    std::cout << formatOutput("KRB5 config profile ", OutputType::kHighlight)
              << "resolved as: " << std::endl;
    std::cout << formatOutput(krb5Env.getProfile().toString(), OutputType::kImportant);
    report.closeSection("KRB5 config profile resolved without errors.");

    StringData ktraceLogPath = krb5Env.getTraceLocation();
    if (!ktraceLogPath.empty() && ktraceLogPath != "/dev/stdout") {
        report.openSection("Fetching KRB5 Log");
        std::ifstream ktraceLog(ktraceLogPath.toString());
        ktraceLogContents = std::string(std::istreambuf_iterator<char>(ktraceLog),
                                        std::istreambuf_iterator<char>());
        if (ktraceLogContents.empty()) {
            ktraceLogContents = "<empty>";
        }
        std::cout << "Displaying contents of KRB5_TRACE file from '" << ktraceLogPath << "'\n"
                  << ktraceLogContents << std::endl;
        report.closeSection("");
    }

    ScopeGuard resetEnv = [&krb5Env] {
        // Since we set the KRB5_CLIENT_KTNAME to the same value as KRB5_KTNAME in server mode, we
        // should set it back here
        int result = setenv(kKRB5_CLIENT_KTNAME.rawData(),
                            krb5Env.getEnvironmentValue(kKRB5_CLIENT_KTNAME).c_str(),
                            1);
        if (result != 0) {
            std::cout << "Could not set environment variable kKRB5_CLIENT_KTNMAE back to global "
                         "environment value."
                      << std::endl;
        }
    };

    if (krb5Env.isClientConnection()) {
        resetEnv.dismiss();
        report.openSection("Attempting client half of GSSAPI conversation");
    } else {
        // We override the client keytab on server mode so that GSSAPI will be tricked into using it
        // when trying to obtain credentials. This is done after collecting the environment
        // variables to avoid confusing the user. See SERVER-50633 for more context.
        int result = setenv(
            kKRB5_CLIENT_KTNAME.rawData(), krb5Env.getEnvironmentValue(kKRB5_KTNAME).c_str(), 1);
        if (result != 0) {
            std::cout << "Could not set environment variable KRB5_CLIENT_KTNAME to test GSSAPI "
                         "handshake as service connection."
                      << std::endl;
        }
        report.openSection("Attempting to initiate security context with service credentials");
    }

    std::string initiator = krb5Env.isClientConnection()
        ? globalKerberosToolOptions->username
        : globalKerberosToolOptions->getHostbasedService();

    performGSSAPIHandshake(report, globalKerberosToolOptions->connectionType, initiator);

    if (krb5Env.isClientConnection()) {
        report.closeSection("Client half of GSSAPI conversation completed successfully.");
    } else {
        report.closeSection("Security context initiated successfully.");
    }

#endif
    return 0;
}

}  // namespace

#ifndef _WIN32
KerberosEnvironment::KerberosEnvironment(krb5_context krb5Context,
                                         KerberosToolOptions::ConnectionType connectionType)
    : _variables{},
      _krb5Context(krb5Context),
      _krb5Profile(KRB5Profile::create(_krb5Context)),
      _keytab(_krb5Context, connectionType),
      _credentialsCache(krb5Context),
      _connectionType(connectionType) {

    for (StringData envVar :
         {kKRB5CCNAME, kKRB5_KTNAME, kKRB5_CONFIG, kKRB5_TRACE, kKRB5_CLIENT_KTNAME}) {
        _variables.insert_or_assign(envVar, getEnvironmentValue(envVar));
    }
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
        ScopeGuard endSeq = [&] {
            if (ktCursor != nullptr) {
                if (krb5_kt_end_seq_get(_krb5Context, _krb5Keytab, &ktCursor) != 0) {
                    LOGV2_WARNING(24242, "Could not end keytab iteration sequence properly.");
                }
            }
        };
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

}  // namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through argv().  This enables
// decryptToolMain() to process UTF-8 encoded arguments without regard
// to platform.
int wmain(int argc, wchar_t** argvW) {
    mongo::WindowsCommandLine wcl(argc, argvW);
    int exitCode = mongo::kerberosToolMain(argc, wcl.argv());
    mongo::quickExit(exitCode);
}
#else
int main(int argc, char** argv) {
    int exitCode = mongo::kerberosToolMain(argc, argv);
    mongo::quickExit(exitCode);
}
#endif
