/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include <fmt/format.h>
#include <memory>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/db/auth/cluster_auth_mode.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/service_context.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_component_settings.h"
#include "mongo/logv2/log_manager.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/dns_query.h"
#include "mongo/util/duration.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/net/ocsp/ocsp_manager.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/system_tick_source.h"
#include "mongo/util/text.h"
#include "mongo/util/tick_source.h"
#include "mongo/util/tick_source_mock.h"
#include "mongo/util/version.h"

#include "ldap_options.h"
#include "ldap_tool_options.h"

#include "ldap_manager_impl.h"
#include "ldap_query.h"
#include "ldap_query_config.h"
#include "ldap_resolver_cache.h"
#include "ldap_runner_impl.h"

#include "util/report.h"

namespace mongo {
namespace {
constexpr auto kGlobalCatalogPortLDAP = 3268;
constexpr auto kGlobalCatalogPortLDAPS = 3269;


void getSSLParamsForLdap(SSLParams* params) {
    params->sslPEMKeyFile = "";
    params->sslPEMKeyPassword = "";
    params->sslClusterFile = "";
    params->sslClusterPassword = "";
    params->sslCAFile = globalLDAPParams->serverCAFile;

    params->sslCRLFile = "";

    // Copy the rest from the global SSL manager options.
    params->sslFIPSMode = sslGlobalParams.sslFIPSMode;

    // Not allowing invalid certificates for now
    params->sslAllowInvalidCertificates = false;
    params->sslAllowInvalidHostnames = false;

    params->sslDisabledProtocols =
        std::vector({SSLParams::Protocols::TLS1_0, SSLParams::Protocols::TLS1_1});
}

Status checkConnectivity(const LDAPHost& host,
                         bool checkTLSConnection,
                         boost::optional<std::string> canonicalHostName) {
    try {
        std::string hostName = host.getName();
        int port = host.getPort();
        SockAddr sockAddr = SockAddr::create(hostName, port, AF_UNSPEC);
        Socket sock;
        size_t attempt = 0;
        constexpr size_t kMaxAttempts = 5;

        while (!sock.connect(sockAddr)) {
            ++attempt;
            if (attempt > kMaxAttempts) {
                return Status(ErrorCodes::HostUnreachable, "Unable to establish TCP connection");
            }
        }

        if (checkTLSConnection) {
            SSLParams params;
            getSSLParamsForLdap(&params);
            std::shared_ptr<SSLManagerInterface> _sslManager =
                SSLManagerInterface::create(params, false);

            if (!sock.secure(_sslManager.get(), hostName)) {
                // Verify if a TLS connection fails because the resolved host is absent from the
                // certificate's subject.
                if (canonicalHostName && sock.secure(_sslManager.get(), canonicalHostName.get())) {
                    return Status(ErrorCodes::SSLHandshakeFailed,
                                  str::stream()
                                      << "Unable to establish TLS connection due to resolved host: "
                                      << hostName << " being absent in the certificate's subject");
                }
                return Status(ErrorCodes::SSLHandshakeFailed, "Unable to establish TLS connection");
            }
        }

    } catch (const DBException& e) {
        return e.toStatus();
    }

    return Status::OK();
}

int ldapToolMain(int argc, char** argv) {
    setupSignalHandlers();
    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));
    startSignalProcessingThread();
    setGlobalServiceContext(ServiceContext::make());

    auto serviceContext = getGlobalServiceContext();
    OCSPManager::start(serviceContext);

    auto userAcquisitionStats = std::make_shared<UserAcquisitionStats>();

    if (globalLDAPToolOptions->debug) {
        logv2::LogManager::global().getGlobalSettings().setMinimumLoggedSeverity(
            logv2::LogComponent::kDefault,
            logv2::LogSeverity::Debug(logv2::LogSeverity::kMaxDebugLevel));
    }

    std::cout << "Running MongoDB LDAP authorization validation checks..." << std::endl
              << "Version: " << mongo::VersionInfoInterface::instance().version() << std::endl
              << std::endl;

    Report report(globalLDAPToolOptions->color);

    report.openSection("Checking that an server has been specified");
    report.checkAssert(
        Report::ResultsAssertion([] { return !globalLDAPParams->serverHosts.empty(); },
                                 "No LDAP servers have been provided"));
    report.closeSection("LDAP server(s) provided in configuration");

    // checking that the dns entries resolve before connecting to the LDAP server
    report.openSection("Checking that the DNS names of the LDAP servers resolve");
    LDAPDNSResolverCache dnsCache;
    report.printItemList([&] {
        std::vector<std::string> summary;
        for (const auto& ldapServer : globalLDAPParams->serverHosts) {
            // If domain is an A record, resolve each individual host
            if (ldapServer.getType() == LDAPHost::Type::kDefault) {
                try {
                    std::vector<std::pair<std::string, Seconds>> dnsRecords =
                        dns::lookupARecords(ldapServer.getName());

                    for (const auto& dnsRecord : dnsRecords) {
                        LDAPHost host(
                            LDAPHost::Type::kDefault, dnsRecord.first, ldapServer.isSSL());
                        auto currLookup = dnsCache.resolve(host);

                        if (currLookup.isOK()) {
                            summary.emplace_back(
                                "LDAP Host: " + ldapServer.getName() +
                                " was successfully resolved to address: " + dnsRecord.first);
                        } else {
                            summary.emplace_back(
                                "LDAP Host: " + ldapServer.getName() +
                                " was NOT successfully resolved to address: " + dnsRecord.first);
                        }
                    }
                    continue;
                } catch (...) {
                    summary.emplace_back("LDAP Host: " + ldapServer.getName() +
                                         " did not resolve to any A record");
                }
            }

            // Resolve canonical SRV or raw SRV hostname
            auto currLookup = dnsCache.resolve(ldapServer);
            if (currLookup.isOK()) {
                summary.emplace_back(
                    "LDAP Host: " + ldapServer.getName() +
                    " was successfully resolved to address: " + currLookup.getValue().getAddress());
            } else {
                // If the host name was not found, check if the user specified the wrong SRV prefix
                if (ldapServer.getType() == LDAPHost::Type::kSRV) {
                    LDAPHost host(
                        LDAPHost::Type::kSRVRaw, ldapServer.getNameAndPort(), ldapServer.isSSL());

                    auto srvRawLookup = dnsCache.resolve(host);

                    if (srvRawLookup.isOK()) {
                        summary.emplace_back(
                            "LDAP Host: " + ldapServer.getName() +
                            " for SRV was NOT successfully resolved but was resolved for "
                            "'srv_raw'. Change the prefix before the server name to 'srv_raw'");
                        continue;
                    }
                } else if (ldapServer.getType() == LDAPHost::Type::kSRVRaw) {
                    LDAPHost host(
                        LDAPHost::Type::kSRV, ldapServer.getNameAndPort(), ldapServer.isSSL());

                    auto srvLookup = dnsCache.resolve(host);

                    if (srvLookup.isOK()) {
                        summary.emplace_back(
                            fmt::format("LDAP Host: {} has no SRV record. A SRV record was found "
                                        "at _ldap._tcp.{} or _ldap._tcp.gc_msdcs.{} instead. "
                                        "Change the prefix before the server name to 'srv'",
                                        ldapServer.getName(),
                                        ldapServer.getName(),
                                        ldapServer.getName()));
                        continue;
                    }
                }

                summary.push_back("LDAP Host: " + ldapServer.getName() +
                                  " was NOT successfully resolved.");
            }
        }
        return summary;
    });

    report.closeSection("All DNS entries resolved");

    // checking information for TLS:
    // 1) If TLS is the specified transport security
    // 2) If a CA file has been provided for TLS usage
    // 3) Testing the Connectivity of each LDAP host
    report.openSection("Checking for TLS usage and connectivity");
    report.printItemList([] {
        bool checkTLSConnection =
            globalLDAPParams->transportSecurity == LDAPTransportSecurityType::kTLS;
        std::vector<std::string> summary;
        for (auto& ldapServer : globalLDAPParams->serverHosts) {
            // Check all resolved dns A records for connectivity and TLS connections
            if (ldapServer.getType() == LDAPHost::Type::kDefault) {
                try {
                    std::vector<std::pair<std::string, Seconds>> dnsRecords =
                        dns::lookupARecords(ldapServer.getName());

                    for (const auto& dnsRecord : dnsRecords) {
                        std::string hostAndPort =
                            dnsRecord.first + ":" + std::to_string(ldapServer.getPort());
                        LDAPHost host(LDAPHost::Type::kDefault, hostAndPort, ldapServer.isSSL());
                        Status status =
                            checkConnectivity(host, checkTLSConnection, ldapServer.getName());

                        if (status.isOK()) {
                            summary.emplace_back(fmt::format(
                                "Sucessfully established {} with DNS-resolved A record: {} from: "
                                "{}",
                                checkTLSConnection ? "TLS connection" : "TCP connection",
                                dnsRecord.first,
                                ldapServer.getName()));
                        } else {
                            summary.emplace_back(ldapServer.getName() +
                                                 " with DNS-resolved A record: " + dnsRecord.first +
                                                 " " + status.codeString() + ": " +
                                                 status.reason());
                        }
                    }
                } catch (...) {
                    summary.emplace_back("LDAP Host: " + ldapServer.getName() +
                                         " did not resolve to any A record");
                }
            }

            // Check canonical hostnames e.g. ldaptest.10gen.cc for connectivity and TLS connections
            Status status = checkConnectivity(ldapServer, checkTLSConnection, boost::none);
            if (status.isOK()) {

                summary.emplace_back(
                    fmt::format("Sucessfully established {} with hostname: {}",
                                (checkTLSConnection ? "TLS connection" : "TCP connection"),
                                ldapServer.getName()));
            } else {
                summary.emplace_back(ldapServer.getName() + " " + status.codeString() + ": " +
                                     status.reason());
            }
        }

        return summary;
    });
    report.closeSection("Finshed checking TLS usage and connectivity");

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
         "To fix this issue, remove the PLAIN mechanism from SASL bind mechanisms, or enable "
         "TLS."},
        Report::FailType::kNonFatalFailure));

    LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                std::move(globalLDAPParams->bindPassword),
                                globalLDAPParams->bindMethod,
                                globalLDAPParams->bindSASLMechanisms,
                                globalLDAPParams->useOSDefaults);
    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                            globalLDAPParams->serverHosts);
    std::unique_ptr<LDAPRunner> runner =
        std::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);


    auto swRootDSEQuery =
        LDAPQueryConfig::createLDAPQueryConfig("?supportedSASLMechanisms?base?(objectclass=*)");
    invariant(swRootDSEQuery.isOK());  // This isn't user configurable, so should never fail

    StatusWith<LDAPEntityCollection> swResult = runner->runQuery(
        LDAPQuery::instantiateQuery(swRootDSEQuery.getValue(), LDAPQueryContext::kLivenessCheck)
            .getValue(),
        getGlobalServiceContext()->getTickSource(),
        userAcquisitionStats);

    report.printItemList([&] {
        std::vector<std::string> results{"UserAcquisitionStats after liveness check: "};
        StringBuilder sb;
        auto tickSource = getGlobalServiceContext()->getTickSource();

        userAcquisitionStats->userCacheAcquisitionStatsToString(&sb, tickSource);
        userAcquisitionStats->ldapOperationStatsToString(&sb, tickSource);

        results.emplace_back(sb.str());

        return results;
    });

    LDAPEntityCollection results;

    // TODO: Determine if the query failed because of LDAP error 50, LDAP_INSUFFICIENT_ACCESS.
    // If no, we don't need to mention that the server might not be allowing anonymous access.
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


    report.openSection("Checking for Active Directory and Global Catalog Usage");
    bool isAdCheckNeeded = true;
    for (const auto& currHost : globalLDAPParams->serverHosts) {
        int port = currHost.getPort();
        if ((port == kGlobalCatalogPortLDAP) || (port == kGlobalCatalogPortLDAPS)) {
            isAdCheckNeeded = false;
            break;
        }
    }

    if (isAdCheckNeeded) {
        LDAPEntityCollection forestResults;
        bool hasForestFunctionality;

        auto rootDSEForestQuery =
            LDAPQueryConfig::createLDAPQueryConfig("?forestFunctionality?base?(objectclass=*)");
        invariant(rootDSEForestQuery.isOK());

        auto initialQueryResults = LDAPQuery::instantiateQuery(rootDSEForestQuery.getValue(),
                                                               LDAPQueryContext::kLivenessCheck);
        invariant(initialQueryResults.isOK());

        StatusWith<LDAPEntityCollection> forestQueryResults =
            runner->runQuery(initialQueryResults.getValue(),
                             getGlobalServiceContext()->getTickSource(),
                             userAcquisitionStats);

        report.printItemList([&] {
            std::vector<std::string> results{"UserAcquisitionStats after AD check: "};
            StringBuilder sb;
            auto tickSource = getGlobalServiceContext()->getTickSource();

            userAcquisitionStats->userCacheAcquisitionStatsToString(&sb, tickSource);
            userAcquisitionStats->ldapOperationStatsToString(&sb, tickSource);

            results.emplace_back(sb.str());

            return results;
        });

        report.checkAssert(Report::ResultsAssertion(
            [&] {
                if (forestQueryResults.isOK()) {
                    forestResults = std::move(forestQueryResults.getValue());
                    return true;
                }
                return false;
            },
            "Could not connect to any of the specified LDAP servers",
            [&] {
                return std::vector<std::string>{
                    str::stream() << "Error: " << forestQueryResults.getStatus().toString(),
                    "The server may be down, or 'security.ldap.servers' or "
                    "'security.ldap.transportSecurity' may be incorrectly configured.",
                    "Alternatively the server may not allow anonymous access to the RootDSE."};
            }));

        LDAPEntityCollection::iterator forestRootDSE = forestResults.find("");
        report.checkAssert(Report::ResultsAssertion(
            [&] {
                if (forestRootDSE != forestResults.end()) {
                    hasForestFunctionality = forestRootDSE->second.find("forestFunctionality") !=
                        forestRootDSE->second.end();
                    return true;
                } else {
                    return false;
                }
            },
            "Was unable to acquire a RootDSE, so could not find out if LDAP is a Microsoft Active "
            "Directory server.",
            std::vector<std::string>{},
            Report::FailType::kNonFatalFailure));

        report.checkAssert(Report::ResultsAssertion(
            [&] { return !hasForestFunctionality; },
            "The LDAP server is a Microsoft Active Directory server but it is not using the Global "
            "Catalog port. Please change the port to 3268 for insecure or 3269 for secure TLS "
            "connections to get better query performance.",
            std::vector<std::string>{},
            Report::FailType::kNonFatalInfo));
    }

    report.closeSection("Done checking for Active Directory and Global Catalog Usage");


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
                                                       globalLDAPToolOptions->password,
                                                       getGlobalServiceContext()->getTickSource(),
                                                       userAcquisitionStats);
        report.printItemList([&] {
            std::vector<std::string> results{
                "UserAcquisitionStats after authenticating to the LDAP server: "};
            StringBuilder sb;
            auto tickSource = getGlobalServiceContext()->getTickSource();

            userAcquisitionStats->userCacheAcquisitionStatsToString(&sb, tickSource);
            userAcquisitionStats->ldapOperationStatsToString(&sb, tickSource);

            results.emplace_back(sb.str());

            return results;
        });
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
            manager.getUserRoles(UserName(globalLDAPToolOptions->user, "$external"),
                                 getGlobalServiceContext()->getTickSource(),
                                 userAcquisitionStats);
        report.printItemList([&] {
            std::vector<std::string> results{"UserAcquisitionStats after user roles query: "};
            StringBuilder sb;
            auto tickSource = getGlobalServiceContext()->getTickSource();

            userAcquisitionStats->userCacheAcquisitionStatsToString(&sb, tickSource);
            userAcquisitionStats->ldapOperationStatsToString(&sb, tickSource);

            results.emplace_back(sb.str());

            return results;
        });
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
                           [](const RoleName& role) { return role.getRole(); });
            return roleStrings;
        });
    }

    /**
     * We are using quickExit here to avoid bad access to userAcquisitionStats.
     **/
    mongo::quickExit(ExitCode::clean);
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
