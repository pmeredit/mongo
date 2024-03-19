/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 */

#include <boost/range/size.hpp>
#include <ostream>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/db/server_options_base.h"
#include "mongo/db/server_options_server_helpers.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/cmdline_utils/censor_cmdline_test.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/options_parser/option_section.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_options.h"

namespace moe = mongo::optionenvironment;

namespace mongo {
namespace {

// ssl_options_server.cpp has an initializer which depends on logging.
// We can stub that dependency out f or unit testing purposes.
MONGO_INITIALIZER(ServerLogRedirection)(InitializerContext*) {}

Status executeInitializer(const std::string& name) try {
    InitializerFunction fn = getGlobalInitializer().getInitializerFunctionForTesting(name);
    uassert(ErrorCodes::InternalError,
            str::stream() << "Initializer node '" << name << "' has no associated function.",
            fn);
    InitializerContext initContext({});
    fn(&initContext);
    return Status::OK();
} catch (const DBException& ex) {
    return ex.toStatus();
}

Status addFIPSServerOptions() {
    Status status = executeInitializer("SSLServerOptionsIDL_Register");
    if (!status.isOK()) {
        return status;
    }
    return executeInitializer("FIPSOptionsIDL_Register");
}

Status storeFIPSServerOptions() {
    auto sslInitializerStatus = executeInitializer("SSLServerOptionsIDL_Store");
    if (sslInitializerStatus.isOK()) {
        sslInitializerStatus = executeInitializer("SSLServerOptions_Post");
        if (!sslInitializerStatus.isOK()) {
            return sslInitializerStatus;
        }
    }

    return executeInitializer("FIPSOptionsIDL_Store");
    // auto fipsInitializerStatus = executeInitializer("FIPSOptionsIDL_Store");
    // if (!fipsInitializerStatus.isOK()) {
    //     return fipsInitializerStatus;
    // }
    // return executeInitializer("FIPSOptionsIDL_Post");
}

class OptionsParserTester : public moe::OptionsParser {
public:
    Status readConfigFile(const std::string& filename,
                          std::string* config,
                          moe::ConfigExpand) override {
        if (filename != _filename) {
            ::mongo::StringBuilder sb;
            sb << "Parser using filename: " << filename
               << " which does not match expected filename: " << _filename;
            return Status(ErrorCodes::InternalError, sb.str());
        }
        *config = _config;
        return Status::OK();
    }
    void setConfig(const std::string& filename, const std::string& config) {
        _filename = filename;
        _config = config;
    }

private:
    std::string _filename;
    std::string _config;
};

TEST(SetupOptions, tlsModeRequired) {
    moe::startupOptions = moe::OptionSection();
    moe::startupOptionsParsed = moe::Environment();

    ASSERT_OK(::mongo::addGeneralServerOptions(&moe::startupOptions));
    ASSERT_OK(addFIPSServerOptions());

    std::string sslPEMKeyFile = "jstests/libs/server.pem";
    std::string sslCAFFile = "jstests/libs/ca.pem";
    std::string sslCRLFile = "jstests/libs/crl.pem";
    std::string sslClusterFile = "jstests/libs/cluster_cert.pem";

    std::vector<std::string> argv;
    argv.push_back("binaryname");
    argv.push_back("--tlsMode");
    argv.push_back("requireTLS");
    argv.push_back("--tlsCertificateKeyFile");
    argv.push_back(sslPEMKeyFile);
    argv.push_back("--tlsCAFile");
    argv.push_back(sslCAFFile);
    argv.push_back("--tlsCRLFile");
    argv.push_back(sslCRLFile);
    argv.push_back("--tlsClusterFile");
    argv.push_back(sslClusterFile);
    argv.push_back("--tlsAllowInvalidHostnames");
    argv.push_back("--tlsAllowInvalidCertificates");
    argv.push_back("--tlsWeakCertificateValidation");
    argv.push_back("--tlsFIPSMode");
    argv.push_back("--tlsCertificateKeyFilePassword");
    argv.push_back("pw1");
    argv.push_back("--tlsClusterPassword");
    argv.push_back("pw2");
    argv.push_back("--tlsDisabledProtocols");
    argv.push_back("TLS1_1");
    argv.push_back("--tlsLogVersions");
    argv.push_back("TLS1_2");

    OptionsParserTester parser;
    ASSERT_OK(parser.run(moe::startupOptions, argv, &moe::startupOptionsParsed));
    ASSERT_OK(storeFIPSServerOptions());

    ASSERT_EQ(::mongo::sslGlobalParams.sslMode.load(), ::mongo::sslGlobalParams.SSLMode_requireSSL);
    ASSERT_EQ(::mongo::sslGlobalParams.sslPEMKeyFile.substr(
                  ::mongo::sslGlobalParams.sslPEMKeyFile.length() - sslPEMKeyFile.length()),
              sslPEMKeyFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslCAFile.substr(
                  ::mongo::sslGlobalParams.sslCAFile.length() - sslCAFFile.length()),
              sslCAFFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslCRLFile.substr(
                  ::mongo::sslGlobalParams.sslCRLFile.length() - sslCRLFile.length()),
              sslCRLFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslClusterFile.substr(
                  ::mongo::sslGlobalParams.sslClusterFile.length() - sslClusterFile.length()),
              sslClusterFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslAllowInvalidHostnames, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslAllowInvalidCertificates, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslWeakCertificateValidation, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslFIPSMode, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslPEMKeyPassword, "pw1");
    ASSERT_EQ(::mongo::sslGlobalParams.sslClusterPassword, "pw2");
    ASSERT_EQ(static_cast<int>(::mongo::sslGlobalParams.sslDisabledProtocols.back()),
              static_cast<int>(::mongo::SSLParams::Protocols::TLS1_1));
    ASSERT_EQ(static_cast<int>(::mongo::sslGlobalParams.tlsLogVersions.back()),
              static_cast<int>(::mongo::SSLParams::Protocols::TLS1_2));
}

TEST(SetupOptions, sslModeRequired) {
    moe::startupOptions = moe::OptionSection();
    moe::startupOptionsParsed = moe::Environment();

    ASSERT_OK(::mongo::addGeneralServerOptions(&moe::startupOptions));
    ASSERT_OK(addFIPSServerOptions());

    std::string sslPEMKeyFile = "jstests/libs/server.pem";
    std::string sslCAFFile = "jstests/libs/ca.pem";
    std::string sslCRLFile = "jstests/libs/crl.pem";
    std::string sslClusterFile = "jstests/libs/cluster_cert.pem";

    std::vector<std::string> argv;
    argv.push_back("binaryname");
    argv.push_back("--sslMode");
    argv.push_back("requireSSL");
    argv.push_back("--sslPEMKeyFile");
    argv.push_back(sslPEMKeyFile);
    argv.push_back("--sslCAFile");
    argv.push_back(sslCAFFile);
    argv.push_back("--sslCRLFile");
    argv.push_back(sslCRLFile);
    argv.push_back("--sslClusterFile");
    argv.push_back(sslClusterFile);
    argv.push_back("--sslAllowInvalidHostnames");
    argv.push_back("--sslAllowInvalidCertificates");
    argv.push_back("--sslWeakCertificateValidation");
    argv.push_back("--sslFIPSMode");
    argv.push_back("--sslPEMKeyPassword");
    argv.push_back("pw1");
    argv.push_back("--sslClusterPassword");
    argv.push_back("pw2");
    argv.push_back("--sslDisabledProtocols");
    argv.push_back("TLS1_1");
    argv.push_back("--tlsLogVersions");
    argv.push_back("TLS1_0");

    OptionsParserTester parser;
    ASSERT_OK(parser.run(moe::startupOptions, argv, &moe::startupOptionsParsed));
    ASSERT_OK(storeFIPSServerOptions());

    ASSERT_EQ(::mongo::sslGlobalParams.sslMode.load(), ::mongo::sslGlobalParams.SSLMode_requireSSL);
    ASSERT_EQ(::mongo::sslGlobalParams.sslPEMKeyFile.substr(
                  ::mongo::sslGlobalParams.sslPEMKeyFile.length() - sslPEMKeyFile.length()),
              sslPEMKeyFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslCAFile.substr(
                  ::mongo::sslGlobalParams.sslCAFile.length() - sslCAFFile.length()),
              sslCAFFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslCRLFile.substr(
                  ::mongo::sslGlobalParams.sslCRLFile.length() - sslCRLFile.length()),
              sslCRLFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslClusterFile.substr(
                  ::mongo::sslGlobalParams.sslClusterFile.length() - sslClusterFile.length()),
              sslClusterFile);
    ASSERT_EQ(::mongo::sslGlobalParams.sslAllowInvalidHostnames, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslAllowInvalidCertificates, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslWeakCertificateValidation, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslFIPSMode, true);
    ASSERT_EQ(::mongo::sslGlobalParams.sslPEMKeyPassword, "pw1");
    ASSERT_EQ(::mongo::sslGlobalParams.sslClusterPassword, "pw2");
    ASSERT_EQ(static_cast<int>(::mongo::sslGlobalParams.sslDisabledProtocols.back()),
              static_cast<int>(::mongo::SSLParams::Protocols::TLS1_1));
    ASSERT_EQ(static_cast<int>(::mongo::sslGlobalParams.tlsLogVersions.back()),
              static_cast<int>(::mongo::SSLParams::Protocols::TLS1_0));
}

}  // namespace
}  // namespace mongo
