/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/service_context_noop.h"
#include "mongo/logger/logger.h"
#include "mongo/stdx/memory.h"
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

namespace mongo {

namespace {

MONGO_INITIALIZER(SetGlobalEnvironment)(InitializerContext* context) {
    setGlobalServiceContext(stdx::make_unique<ServiceContextNoop>());
    return Status::OK();
}

struct ResultsAssertion {
    using Conditional = stdx::function<bool()>;

    ResultsAssertion(Conditional assertedCondition,
                     std::string failureMessage,
                     stdx::function<std::vector<std::string>()> failureBulletGenerator,
                     bool isFatal = true)
        : assertedCondition(std::move(assertedCondition)),
          failureMessage(std::move(failureMessage)),
          failureBulletGenerator(std::move(failureBulletGenerator)),
          isFatal(isFatal) {}
    ResultsAssertion(Conditional assertedCondition,
                     std::string failureMessage,
                     std::vector<std::string> failureBullets = std::vector<std::string>{},
                     bool isFatal = true)
        : ResultsAssertion(std::move(assertedCondition),
                           std::move(failureMessage),
                           [failureBullets] { return failureBullets; },
                           isFatal) {}

    Conditional assertedCondition;
    std::string failureMessage;
    stdx::function<std::vector<std::string>()> failureBulletGenerator;
    bool isFatal;
};

/**
 * A segment of a formatted report. This segment will have a Title, and optionally some
 * informational bullet points. An object of this class will execute a series of tests. If any
 * test fails, the segment in the report will conclude with a failure message, and informational
 * bullet points describing the problem and suggesting possible solutions. If all tests pass,
 * the segment in the report will conclude with an affirmative message.
 *
 * Example:
 *
 * Title...
 *     * Informational
 *     * Bullet Points
 * [FAIL] Test failed
 *     * Here's why:
 *     * Advice about how to fix the problem
 *
 */
class Report {
public:
    void openSection(std::string testName) {
        _nonFatalAssertTriggered = false;
        std::cout << testName << "..." << std::endl;
    }

    void printItemList(stdx::function<std::vector<std::string>()> infoBulletGenerator) {
        if (_nonFatalAssertTriggered) {
            return;
        }
        auto infoBullets = infoBulletGenerator();
        for (const std::string& infoBullet : infoBullets) {
            std::cout << "\t* " << infoBullet << std::endl;
        }
    }

    void checkAssert(ResultsAssertion&& assert) {
        if (_nonFatalAssertTriggered || assert.assertedCondition()) {
            return;
        }

        std::cout << failString() << assert.failureMessage << std::endl;
        auto failureBullets = assert.failureBulletGenerator();
        for (const std::string& bullet : failureBullets) {
            std::cout << "\t* " << bullet << std::endl;
        }
        std::cout << std::endl;

        if (assert.isFatal) {
            // Fatal assertion failures are logged and the test program terminates immediately
            quickExit(-1);
        } else {
            // Non fatal failures will prevent the rest of the section from being executed
            _nonFatalAssertTriggered = true;
        }
    }

    void closeSection(StringData successMessage) {
        if (!_nonFatalAssertTriggered) {
            std::cout << okString() << successMessage << std::endl << std::endl;
        }
    }

private:
    StringData okString() {
        if (globalLDAPToolOptions->color) {
            return "[\x1b[32mOK\x1b[0m] ";
        }
        return "[OK] ";
    }

    StringData failString() {
        if (globalLDAPToolOptions->color) {
            return "[\x1b[31mFAIL\x1b[0m] ";
        }
        return "[FAIL] ";
    }

    // Tracks whether an error has occurred which should short circuit
    // the remainder of the assert checks for the current section.
    bool _nonFatalAssertTriggered = false;
};

int ldapToolMain(int argc, char* argv[], char** envp) {
    setupSignalHandlers();
    runGlobalInitializersOrDie(argc, argv, envp);
    startSignalProcessingThread();

    if (globalLDAPToolOptions->debug) {
        logger::globalLogDomain()->setMinimumLoggedSeverity(logger::LogSeverity::Debug(255));
    }

    std::cout << "Running MongoDB LDAP authorization validation checks..." << std::endl
              << "Version: " << mongo::VersionInfoInterface::instance().version() << std::endl
              << std::endl;

    Report report;


    report.openSection("Checking that an LDAP server has been specified");
    report.checkAssert(ResultsAssertion([] { return !globalLDAPParams->serverHosts.empty(); },
                                        "No LDAP servers have been provided"));
    report.closeSection("LDAP server found");


    report.openSection("Connecting to LDAP server");
    LDAPBindOptions bindOptions(globalLDAPParams->bindUser,
                                std::move(globalLDAPParams->bindPassword),
                                globalLDAPParams->bindMethod,
                                globalLDAPParams->bindSASLMechanisms,
                                globalLDAPParams->useOSDefaults);
    LDAPConnectionOptions connectionOptions(globalLDAPParams->connectionTimeout,
                                            globalLDAPParams->serverHosts,
                                            globalLDAPParams->transportSecurity);
    std::unique_ptr<LDAPRunner> runner =
        stdx::make_unique<LDAPRunnerImpl>(bindOptions, connectionOptions);

    auto swRootDSEQuery =
        LDAPQueryConfig::createLDAPQueryConfig("?supportedSASLMechanisms?base?(objectclass=*)");
    invariant(swRootDSEQuery.isOK());  // This isn't user configurable, so should never fail

    StatusWith<LDAPEntityCollection> swResult =
        runner->runQuery(LDAPQuery::instantiateQuery(swRootDSEQuery.getValue()).getValue());

    LDAPEntityCollection results;

    // TODO: Determine if the query failed because of LDAP error 50, LDAP_INSUFFICIENT_ACCESS. If
    // no, we don't need to mention that the server might not be allowing anonymous access.
    report.checkAssert(ResultsAssertion(
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

        report.checkAssert(ResultsAssertion(
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
            false));

        report.checkAssert(ResultsAssertion(
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
            false));

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


        report.checkAssert(ResultsAssertion(
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


    auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfigWithUserName(
        globalLDAPParams->userAcquisitionQueryTemplate);

    // TODO: Maybe change how the userToDNMapping is parsed so we can just check for empty?
    std::string defaultNameMapper = "[{match: \"(.+)\", substitution: \"{0}\"}]";
    auto swMapper = InternalToLDAPUserNameMapper::createNameMapper(defaultNameMapper);
    if (globalLDAPParams->userToDNMapping != defaultNameMapper) {
        report.openSection("Parsing MongoDB to LDAP DN mappings");
        swMapper =
            InternalToLDAPUserNameMapper::createNameMapper(globalLDAPParams->userToDNMapping);

        report.checkAssert(ResultsAssertion(
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
        report.checkAssert(ResultsAssertion([&] { return authRes.isOK(); },
                                            str::stream() << "Failed to authenticate "
                                                          << globalLDAPToolOptions->user
                                                          << " to LDAP server",
                                            {authRes.toString()}));
        report.closeSection("Successful authentication performed");
    }

    report.openSection("Checking if LDAP authorization has been enabled by configuration");
    report.checkAssert(ResultsAssertion(
        [] { return globalLDAPParams->isLDAPAuthzEnabled(); },
        "LDAP authorization is not enabled, the configuration will require internal users to be "
        "maintained",
        {"Make sure you have 'security.ldap.authz.queryTemplate' in your configuration"},
        false));
    report.closeSection("LDAP authorization enabled");

    if (globalLDAPParams->isLDAPAuthzEnabled()) {
        report.openSection("Parsing LDAP query template");
        report.checkAssert(ResultsAssertion(
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
        report.checkAssert(
            ResultsAssertion([&] { return swRoles.isOK(); },
                             "Unable to acquire roles",
                             [&] {
                                 return std::vector<std::string>{
                                     str::stream() << "Error: " << swRoles.getStatus().toString()};
                             }));
        report.closeSection("Successfully acquired the following roles:");
        report.printItemList([&] {
            std::vector<std::string> roleStrings;
            std::transform(swRoles.getValue().begin(),
                           swRoles.getValue().end(),
                           std::back_inserter(roleStrings),
                           [](const RoleName& role) { return role.toString(); });
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
// and makes them available through the argv() and envp() members.  This enables decryptToolMain()
// to process UTF-8 encoded arguments and environment variables without regard to platform.
int wmain(int argc, wchar_t* argvW[], wchar_t* envpW[]) {
    mongo::WindowsCommandLine wcl(argc, argvW, envpW);
    int exitCode = mongo::ldapToolMain(argc, wcl.argv(), wcl.envp());
    mongo::quickExit(exitCode);
}
#else
int main(int argc, char* argv[], char** envp) {
    int exitCode = mongo::ldapToolMain(argc, argv, envp);
    mongo::quickExit(exitCode);
}
#endif
