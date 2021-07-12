/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <memory>
#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_component_settings.h"
#include "mongo/logv2/log_manager.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/util/dns_query.h"
#include "mongo/util/invariant.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/text.h"
#include "mongo/util/version.h"

#include "ldap_options.h"
#include "ldap_tool_options.h"

#include "ldap_manager_impl.h"
#include "ldap_query.h"
#include "ldap_query_config.h"
#include "ldap_runner_impl.h"

#include "util/report.h"

namespace mongo {

namespace {

int ldapToolMain(int argc, char** argv) {
    setupSignalHandlers();
    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));
    startSignalProcessingThread();

    if (globalLDAPToolOptions->debug) {
        logv2::LogManager::global().getGlobalSettings().setMinimumLoggedSeverity(
            logv2::LogComponent::kDefault,
            logv2::LogSeverity::Debug(logv2::LogSeverity::kMaxDebugLevel));
    }

    std::cout << "Running MongoDB LDAP authorization validation checks..." << std::endl
              << "Version: " << mongo::VersionInfoInterface::instance().version() << std::endl
              << std::endl;

    Report report(globalLDAPToolOptions->color);


    report.openSection("Checking that an LDAP server has been specified");
    report.checkAssert(
        Report::ResultsAssertion([] { return !globalLDAPParams->serverHosts.empty(); },
                                 "No LDAP servers have been provided"));
    report.closeSection("LDAP server(s) provided in configuration");

    // checking that the dns entries resolve before connecting to the LDAP server
    report.openSection("Checking that the DNS names of the LDAP servers resolve");
    report.printItemList([&] {
        std::vector<std::string> summary;
        for (const auto& ldapServer : globalLDAPParams->serverHosts) {
            try {
                std::vector<std::string> res = dns::lookupARecords(ldapServer);
                if (!res.empty()) {
                    summary.push_back("LDAP Host: " + ldapServer +
                                      " was successfully resolved to address: " + res.front());
                } else {
                    summary.push_back("LDAP Host: " + ldapServer +
                                      " was NOT successfully resolved.");
                }
            } catch (ExceptionFor<ErrorCodes::DNSHostNotFound>&) {
                summary.push_back("LDAP Host: " + ldapServer + " was NOT successfully resolved.");
            }
        }
        return summary;
    });


    report.closeSection("All DNS entries resolved");

    report.openSection("Connecting to LDAP server");
    // produce warning if either:
    // 1) connection to LDAP server is not using TLS and the bind method is simple OR
    // 2) connection to LDAP server is not using TLS and the bind method is SASL PLAIN
    report.checkAssert(Report::ResultsAssertion(
        [] {
            return !((globalLDAPParams->transportSecurity != LDAPTransportSecurityType::kTLS) &&
                     (globalLDAPParams->bindMethod == LDAPBindType::kSimple));
        },
        "Attempted to bind to LDAP server without TLS with a plaintext password.",
        {"Sending a password over a network in plaintext is insecure.",
         "To fix this issue, enable TLS or switch to a different LDAP bind mechanism."},
        Report::FailType::kNonFatalFailure));
    report.checkAssert(Report::ResultsAssertion(
        [] {
            return !(((globalLDAPParams->transportSecurity != LDAPTransportSecurityType::kTLS) &&
                      globalLDAPParams->bindMethod == LDAPBindType::kSasl) &&
                     (globalLDAPParams->bindSASLMechanisms.find("PLAIN") != std::string::npos));
        },
        "Attempted to bind to LDAP server without TLS using SASL PLAIN.",
        {"Sending a password over a network in plaintext is insecure.",
         "To fix this issue, remove the PLAIN mechanism from SASL bind mechanisms, or enable TLS."},
        Report::FailType::kNonFatalFailure));

    LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                std::move(globalLDAPParams->bindPassword),
                                globalLDAPParams->bindMethod,
                                globalLDAPParams->bindSASLMechanisms,
                                globalLDAPParams->useOSDefaults);
    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                            globalLDAPParams->serverHosts,
                                            globalLDAPParams->transportSecurity);
    std::unique_ptr<LDAPRunner> runner =
        std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);


    auto swRootDSEQuery =
        LDAPQueryConfig::createLDAPQueryConfig("?supportedSASLMechanisms?base?(objectclass=*)");
    invariant(swRootDSEQuery.isOK());  // This isn't user configurable, so should never fail

    StatusWith<LDAPEntityCollection> swResult = runner->runQuery(
        LDAPQuery::instantiateQuery(swRootDSEQuery.getValue(), LDAPQueryContext::kLivenessCheck)
            .getValue());

    LDAPEntityCollection results;

    // TODO: Determine if the query failed because of LDAP error 50, LDAP_INSUFFICIENT_ACCESS. If
    // no, we don't need to mention that the server might not be allowing anonymous access.
    report.checkAssert(Report::ResultsAssertion(
        [&] {
            if (swResult.isOK()) {
                results = std::move(swResult.getValue());
                return true;
            }
            return false;
        },
        "Could not connect to any of the specified LDAP servers",
        [&] {
            return std::vector<std::string>{
                str::stream() << "Error: " << swResult.getStatus().toString(),
                "The server may be down, or 'security.ldap.servers' or "
                "'security.ldap.transportSecurity' may be incorrectly configured.",
                "Alternatively the server may not allow anonymous access to the RootDSE."};
        }));
    LDAPEntityCollection::iterator rootDSE = results.find("");
    report.closeSection("Connected to LDAP server");


    if (globalLDAPParams->bindMethod == LDAPBindType::kSasl) {
        report.openSection("Checking that SASL mechanisms are supported by server...");

        StringSplitter splitter(globalLDAPParams->bindSASLMechanisms.c_str(), ",");
        std::vector<std::string> requestedSASLMechanisms = splitter.split();

        LDAPAttributeKeyValuesMap::iterator supportedSASLMechanisms;
        std::vector<std::string> remoteMechanismVector;

        report.checkAssert(Report::ResultsAssertion(
            [&] {
                if (rootDSE != results.end()) {
                    supportedSASLMechanisms = rootDSE->second.find("supportedSASLMechanisms");
                    return true;
                } else {
                    return false;
                }
            },
            "Was unable to acquire a RootDSE, so couldn't compare SASL mechanisms",
            std::vector<std::string>{},
            Report::FailType::kNonFatalFailure));

        report.checkAssert(Report::ResultsAssertion(
            [&] {
                if (supportedSASLMechanisms != rootDSE->second.end()) {
                    remoteMechanismVector = supportedSASLMechanisms->second;
                    return true;
                } else {
                    return false;
                }
            },
            "Server did not return supportedSASLMechanisms in its RootDSE",
            std::vector<std::string>{},
            Report::FailType::kNonFatalFailure));

        report.printItemList([&] {
            std::vector<std::string> results{"Server supports the following SASL mechanisms: "};
            for (const std::string& mechanism : remoteMechanismVector) {
                std::stringstream ss;
                ss << mechanism;
                if (std::find(requestedSASLMechanisms.begin(),
                              requestedSASLMechanisms.end(),
                              mechanism) != requestedSASLMechanisms.end()) {
                    ss << "(requested)";
                }
                results.emplace_back(ss.str());
            }
            return results;
        });


        report.checkAssert(Report::ResultsAssertion(
            [&] {
                return std::find_first_of(requestedSASLMechanisms.begin(),
                                          requestedSASLMechanisms.end(),
                                          remoteMechanismVector.begin(),
                                          remoteMechanismVector.end()) !=
                    requestedSASLMechanisms.end();
            },
            "Server does not support any requested SASL mechanism"));

        report.closeSection("Server supports at least one requested SASL mechanism");
    }


    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserNameAndAttributeTranform(
        globalLDAPParams->userAcquisitionQueryTemplate);

    // TODO: Maybe change how the userToDNMapping is parsed so we can just check for empty?
    std::string defaultNameMapper = "[]";
    auto swMapper = InternalToLDAPUserNameMapper::createNameMapper(defaultNameMapper);
    if (globalLDAPParams->userToDNMapping != defaultNameMapper) {
        report.openSection("Parsing MongoDB to LDAP DN mappings");
        swMapper =
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping);

        report.checkAssert(Report::ResultsAssertion(
            [&] { return swMapper.isOK(); },
            "Unable to parse the MongoDB username to LDAP DN map",
            [&] {
                return std::vector<std::string>{
                    "This mapping is a transformation applied to MongoDB authentication names "
                    "before they are substituted into the query template.",
                    str::stream() << "Error message: " << swMapper.getStatus().toString()};
            }));
        report.closeSection("MongoDB to LDAP DN mappings appear to be valid");
    }

    LDAPManagerImpl manager(
        std::move(runner), std::move(swQueryParameters.getValue()), std::move(swMapper.getValue()));

    if (!globalLDAPToolOptions->password->empty()) {
        report.openSection("Attempting to authenticate against the LDAP server");
        Status authRes = manager.verifyLDAPCredentials(globalLDAPToolOptions->user,
                                                       globalLDAPToolOptions->password);
        report.checkAssert(Report::ResultsAssertion([&] { return authRes.isOK(); },
                                                    str::stream() << "Failed to authenticate "
                                                                  << globalLDAPToolOptions->user
                                                                  << " to LDAP server",
                                                    {authRes.toString()}));
        report.closeSection("Successful authentication performed");
    }

    report.openSection("Checking if LDAP authorization has been enabled by configuration");
    report.checkAssert(Report::ResultsAssertion(
        [] { return globalLDAPParams->isLDAPAuthzEnabled(); },
        "LDAP authorization is not enabled, the configuration will require internal users to be "
        "maintained",
        {"Make sure you have 'security.ldap.authz.queryTemplate' in your configuration"},
        Report::FailType::kNonFatalInfo));
    report.closeSection("LDAP authorization enabled");

    if (globalLDAPParams->isLDAPAuthzEnabled()) {
        report.openSection("Parsing LDAP query template");
        report.checkAssert(Report::ResultsAssertion(
            [&] { return swQueryParameters.isOK(); },
            "Unable to parse the LDAP query configuration template",
            [&] {
                return std::vector<std::string>{
                    "When a user authenticates to MongoDB, the username they authenticate with "
                    "will be "
                    "substituted into this URI. The resulting query will be executed against the "
                    "LDAP "
                    "server to obtain that user's roles.",
                    "Make sure your query template is an RFC 4516 relative URI. This will look "
                    "something like <baseDN>[?<attributes>[?<scope>[?<filter>]]], where all "
                    "bracketed "
                    "placeholders are replaced with their proper values.",
                    str::stream() << "Error message: " << swQueryParameters.getStatus().toString(),
                };
            }));
        report.closeSection("LDAP query configuration template appears valid");

        report.openSection("Executing query against LDAP server");
        StatusWith<std::vector<RoleName>> swRoles =
            manager.getUserRoles(UserName(globalLDAPToolOptions->user, "$external"));
        report.checkAssert(Report::ResultsAssertion(
            [&] { return swRoles.isOK(); },
            "Unable to acquire roles",
            [&] {
                return std::vector<std::string>{str::stream()
                                                << "Error: " << swRoles.getStatus().toString()};
            }));
        report.closeSection("Successfully acquired the following roles on the 'admin' database:");
        report.printItemList([&] {
            std::vector<std::string> roleStrings;
            std::transform(swRoles.getValue().begin(),
                           swRoles.getValue().end(),
                           std::back_inserter(roleStrings),
                           [](const RoleName& role) { return role.getRole().toString(); });
            return roleStrings;
        });
    }
    return 0;
}

}  // namespace
}  // namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through argv().  This enables decryptToolMain()
// to process UTF-8 encoded arguments without regard to platform.
int wmain(int argc, wchar_t** argvW) {
    mongo::WindowsCommandLine wcl(argc, argvW);
    int exitCode = mongo::ldapToolMain(argc, wcl.argv());
    mongo::quickExit(exitCode);
}
#else
int main(int argc, char** argv) {
    int exitCode = mongo::ldapToolMain(argc, argv);
    mongo::quickExit(exitCode);
}
#endif
