/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl

#include <cstdlib>
#include <memory>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/client/sasl_client_session.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/auth/sasl_mechanism_registry.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/concurrency/locker_noop_client_observer.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/errno_util.h"
#include "mongo/util/scopeguard.h"

#include "cyrus_sasl_authentication_session.h"
#include "util/gssapi_helpers.h"

/**
 * This test may require the hostname contained in mockHostName to canonicalize to
 * "localhost" or "localhost.localdomain".
 * If this test fails, ensure that "localhost" is the first hostname for 127.0.0.1
 * and ::1 in /etc/hosts.
 */

namespace {
using namespace mongo;

const std::string mockHostName = "localhost";
const std::string mockServiceName = "mockservice";
const std::string userName = "mockuser@LDAPTEST.10GEN.CC";
char krb5ccFile[] = "mongotest-krb5cc-XXXXXX";

std::string getAbsolutePath(const char* path) {
    char* absolutePath = realpath(path, nullptr);
    fassert(4016, absolutePath);
    std::string result = absolutePath;
    free(absolutePath);
    return result;
}

/**
 * Sets up stock environment variables for the Kerberos library.
 */
void setupEnvironment() {
    // Set kerberos config file to use.
    fassert(4017, !setenv("KRB5_CONFIG", getAbsolutePath("jstests/libs/mockkrb5.conf").c_str(), 1));

    // Set keytab containing keys used by the server side of authentication.
    fassert(4015,
            !setenv("KRB5_KTNAME", getAbsolutePath("jstests/libs/mockservice.keytab").c_str(), 1));

    // Set keytab containing keys used by the client side of authentication.
    fassert(
        4018,
        !setenv("KRB5_CLIENT_KTNAME", getAbsolutePath("jstests/libs/mockuser.keytab").c_str(), 1));

    // Store cached credentials in memory
    fassert(4019, !setenv("KRB5CCNAME", "MEMORY", 1));
}

/**
 * Sets up environment for legacy Kerberos libraries that do not support new
 * credential caches. This will preload the client credential cache with tickets so
 * the client can authenticate as "userName".
 */
void setupLegacyEnvironment() {
    fassert(51229, !setenv("KRB5CCNAME", ("FILE:" + getAbsolutePath(krb5ccFile)).c_str(), 1));

    const pid_t child = fork();
    fassert(4020, child >= 0);
    if (child == 0) {
        int er = execlp("kinit",
                        "kinit",
                        "-k",
                        "-t",
                        "jstests/libs/mockuser.keytab",
                        "-c",
                        krb5ccFile,
                        userName.c_str(),
                        nullptr);
        Status s = Status::OK();
        if (er != 0) {
            s = Status(ErrorCodes::InternalError, errnoWithPrefix("cannot execute \"kinit\""));
        }
        fassert(4021, s);
    }
    int waitStatus;
    pid_t waitedPid;
    while (-1 == (waitedPid = waitpid(child, &waitStatus, 0))) {
        fassert(4022, EINTR == errno);
    }
    if (WIFSIGNALED(waitStatus) || (WIFEXITED(waitStatus) && (WEXITSTATUS(waitStatus) != 0))) {
        fassertFailed(4023);
    }
}

}  // namespace

int main(int argc, char** argv) {
    // Set up *nix-based kerberos.
    if (!mkstemp(krb5ccFile)) {
        LOGV2(24211,
              "Failed to make credential cache with template {krb5ccFile}; {strerror_errno} "
              "({errno})",
              "krb5ccFile"_attr = krb5ccFile,
              "strerror_errno"_attr = strerror(errno),
              "errno"_attr = errno);
        return EXIT_FAILURE;
    }
    ScopeGuard unlinkGuard = [] { unlink(krb5ccFile); };
    setupEnvironment();

    {
        OM_uint32 minorStatus, majorStatus;

        GSSName desiredName;
        gss_buffer_desc nameBuffer;
        nameBuffer.value = const_cast<char*>(userName.c_str());
        nameBuffer.length = userName.size();
        majorStatus =
            gss_import_name(&minorStatus, &nameBuffer, GSS_C_NT_USER_NAME, desiredName.get());
        fassert(51227, majorStatus == GSS_S_COMPLETE);

        GSSCredId credentialHandle;
        majorStatus = gss_acquire_cred(&minorStatus,
                                       *desiredName.get(),
                                       GSS_C_INDEFINITE,
                                       GSS_C_NO_OID_SET,
                                       GSS_C_INITIATE,
                                       credentialHandle.get(),
                                       nullptr,
                                       nullptr);
        if (majorStatus != GSS_S_COMPLETE) {
            LOGV2(24212,
                  "Legacy Kerberos implementation detected, falling back to kinit generated "
                  "credential cache: {getGssapiErrorString_majorStatus_minorStatus}",
                  "getGssapiErrorString_majorStatus_minorStatus"_attr =
                      getGssapiErrorString(majorStatus, minorStatus));
            setupLegacyEnvironment();
        }
    }

    saslGlobalParams.authenticationMechanisms.push_back("GSSAPI");
    saslGlobalParams.serviceName = mockServiceName;
    saslGlobalParams.hostName = mockHostName;

    runGlobalInitializersOrDie(std::vector<std::string>(argv, argv + argc));

    {
        auto service = ServiceContext::make();
        SASLServerMechanismRegistry& registry = SASLServerMechanismRegistry::get(service.get());
        auto swMechanism = registry.getServerMechanism("GSSAPI", "$external");

        if (!swMechanism.isOK()) {
            LOGV2(24213,
                  "Failed to smoke server mechanism from registry.  {swMechanism_getStatus}",
                  "swMechanism_getStatus"_attr = swMechanism.getStatus());
            return EXIT_FAILURE;
        }
    }

    try {
        CyrusGSSAPIServerMechanism mechanism("$external");
    } catch (...) {
        LOGV2(24214,
              "Failed to directly smoke server mechanism.  {exceptionToStatus}",
              "exceptionToStatus"_attr = exceptionToStatus());
        return EXIT_FAILURE;
    }

    return unittest::Suite::run(std::vector<std::string>(), "", "", 1);
}

namespace mongo {
namespace {

class SaslConversationGssapi : public ServiceContextTest {
public:
    SaslConversationGssapi();

    ServiceContext::UniqueOperationContext opCtx;
    AuthorizationManager* authManager;
    std::unique_ptr<AuthorizationSession> authSession;
    std::unique_ptr<SaslClientSession> client;
    std::unique_ptr<ServerMechanismBase> server;
    const std::string mechanism;

protected:
    void assertConversationFailure();
};

SaslConversationGssapi::SaslConversationGssapi() : mechanism("GSSAPI") {
    auto service = getServiceContext();
    service->registerClientObserver(std::make_unique<LockerNoopClientObserver>());
    opCtx = makeOperationContext();

    auto tmpAuthManager = AuthorizationManager::create(service);
    authSession = tmpAuthManager->makeAuthorizationSession();
    authManager = tmpAuthManager.get();
    AuthorizationManager::set(getServiceContext(), std::move(tmpAuthManager));

    client.reset(SaslClientSession::create(mechanism));

    server = std::make_unique<CyrusGSSAPIServerMechanism>("$external");
}

void SaslConversationGssapi::assertConversationFailure() {
    std::string clientMessage;
    Status clientStatus(ErrorCodes::InternalError, "");
    StatusWith<std::string> serverResponse("");
    do {
        clientStatus = client->step(serverResponse.getValue(), &clientMessage);
        if (!clientStatus.isOK()) {
            break;
        }

        serverResponse = server->step(opCtx.get(), clientMessage);
        if (!serverResponse.isOK()) {
            break;
        }
    } while (!client->isSuccess());
    ASSERT_FALSE(serverResponse.isOK() && clientStatus.isOK());
}

TEST_F(SaslConversationGssapi, SuccessfulAuthentication) {
    client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
    client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
    client->setParameter(SaslClientSession::parameterMechanism, mechanism);
    client->setParameter(SaslClientSession::parameterUser, userName);
    ASSERT_OK(client->initialize());

    std::string clientMessage;
    StatusWith<std::string> serverResponse("");
    do {
        ASSERT_OK(client->step(serverResponse.getValue(), &clientMessage));
        serverResponse = server->step(opCtx.get(), clientMessage);
        ASSERT_OK(serverResponse.getStatus());
    } while (!client->isSuccess());
    ASSERT_TRUE(server->isSuccess());
}

TEST_F(SaslConversationGssapi, NoSuchUser) {
    client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
    client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
    client->setParameter(SaslClientSession::parameterMechanism, mechanism);
    client->setParameter(SaslClientSession::parameterUser, "WrongUserName");
    ASSERT_OK(client->initialize());

    assertConversationFailure();
}

TEST_F(SaslConversationGssapi, WrongServiceNameClient) {
    client->setParameter(SaslClientSession::parameterServiceName, "nosuch");
    client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
    client->setParameter(SaslClientSession::parameterMechanism, mechanism);
    client->setParameter(SaslClientSession::parameterUser, userName);
    ASSERT_OK(client->initialize());

    assertConversationFailure();
}

TEST_F(SaslConversationGssapi, WrongServerHostNameClient) {
    client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
    client->setParameter(SaslClientSession::parameterServiceHostname, "badhost.10gen.me");
    client->setParameter(SaslClientSession::parameterMechanism, mechanism);
    client->setParameter(SaslClientSession::parameterUser, userName);
    ASSERT_OK(client->initialize());

    assertConversationFailure();
}

/*
 * The following tests are commented out because the server doesn't care if the client showed up
 * with a ticket for another service principal than the server expected, as long as the server
 * possesses the corresponding ticket.
 *
 * TODO(schwerin): Do we want to go through the trouble of changing this behavior, so that the
 * server only accepts authentications if the client requested to talk to the service principal
 * we expected?
 */
// TEST_F(SaslConversationGssapi, WrongServiceNameServer) {
//     client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
//     client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
//     client.setParameter(SaslClientSession::parameterMechanism, mechanism);
//     client.setParameter(SaslClientSession::parameterUser, userName);
//     ASSERT_OK(client.initialize());

//     ASSERT_OK(server.start("test",
//                            mechanism,
//                            "nosuch",
//                            mockHostName,
//                            1,
//                            true));

//     assertConversationFailure();
// }

// TEST_F(SaslConversationGssapi, WrongServerHostNameServer) {
//     client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
//     client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
//     client.setParameter(SaslClientSession::parameterMechanism, mechanism);
//     client.setParameter(SaslClientSession::parameterUser, userName);
//     ASSERT_OK(client.initialize());

//     ASSERT_OK(server.start("test",
//                            mechanism,
//                            mockServiceName,
//                            "badhost.10gen.me",
//                            1,
//                            true));

//     assertConversationFailure();
// }

}  // namespace
}  // namespace mongo
