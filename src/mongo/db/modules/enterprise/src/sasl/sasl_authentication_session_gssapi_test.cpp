/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include <cstdlib>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/client/sasl_client_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "sasl_authentication_session.h"

namespace mongo {
    int Command::testCommandsEnabled = 1; // To fix compile without needing to link Command code.
}

namespace {
    using namespace mongo;

    const std::string mockHostName = "kdc.10gen.me";
    const std::string mockServiceName = "mockservice";
    const std::string userName = "mockuser@10GEN.ME";
    char krb5ccFile[] = "mongotest-krb5cc-XXXXXX";

    std::string getAbsolutePath(const char* path) {
        char* absolutePath = realpath(path, NULL);
        fassert(4016, absolutePath);
        std::string result = absolutePath;
        free(absolutePath);
        return result;
    }

    /**
     * Sets up environment variables for the MIT Kerberos library.
     */
    void setupEnvironment() {
        // Set kerberos config file to use.
        fassert(4017,
                !setenv("KRB5_CONFIG",
                        getAbsolutePath("jstests/libs/mockkrb5.conf").c_str(),
                        1));

        // Set keytab containing keys used by the server side of authentication.
        fassert(4015,
                !setenv("KRB5_KTNAME",
                        getAbsolutePath("jstests/libs/mockservice.keytab").c_str(),
                        1));

        // Set keytab containing keys used by the client side of authentication.
        fassert(4018,
               !setenv("KRB5_CLIENT_KTNAME",
                       getAbsolutePath("jstests/libs/mockuser.keytab").c_str(),
                       1));

        // Set the name of the file in which to cache the client's kerberos tickets.
        fassert(4019, !setenv("KRB5CCNAME", ("FILE:" + getAbsolutePath(krb5ccFile)).c_str(), 1));
    }

    /**
     * Initializes the client credential cache so that client can authenticate as "userName".
     */
    void initializeClientCredentialCacheOrDie() {
        const pid_t child = fork();
        fassert(4020, child >= 0);
        if (child == 0) {
            fassert(4021, 0 == execlp("kinit",
                                      "kinit",
                                      "-k", "-t", "jstests/libs/mockuser.keytab",
                                      "-c", krb5ccFile,
                                      userName.c_str(),
                                      NULL));
        }
        int waitStatus;
        pid_t waitedPid;
        while (-1 == (waitedPid = waitpid(child, &waitStatus, 0))) {
            fassert(4022, EINTR == errno);
        }
        if (WIFSIGNALED(waitStatus) ||
            (WIFEXITED(waitStatus) && (WEXITSTATUS(waitStatus) != 0))) {

            fassertFailed(4023);
        }
    }
}  // namespace

int main(int argc, char** argv, char** envp) {
    // Set up *nix-based kerberos.
    if (!mkstemp(krb5ccFile)) {
        log() << "Failed to make credential cache with template " << krb5ccFile <<
            "; " << strerror(errno) << " (" << errno << ')' << std::endl;
        return EXIT_FAILURE;
    }
    ON_BLOCK_EXIT(unlink, krb5ccFile);
    setupEnvironment();
    initializeClientCredentialCacheOrDie();

    runGlobalInitializersOrDie(argc, argv, envp);

    Status status = SaslAuthenticationSession::smokeTestMechanism(
            "GSSAPI", mockServiceName, mockHostName);
    if (!status.isOK()) {
        log() << "Failed to smoke server mechanism.  " << status << std::endl;
        return EXIT_FAILURE;
    }

    return unittest::Suite::run(std::vector<std::string>(), "", 1);
}

void mongo::unittest::onCurrentTestNameChange( const std::string &testName ) {}

namespace mongo {
namespace {

    class SaslConversationGssapi : public unittest::Test {
    public:
        SaslConversationGssapi();

        AuthorizationManager authManager;
        AuthorizationSession authSession;
        SaslClientSession client;
        SaslAuthenticationSession server;
        const std::string mechanism;

    protected:
        void assertConversationFailure();
    };

    SaslConversationGssapi::SaslConversationGssapi() :
        authManager(new AuthzManagerExternalStateMock()),
        authSession(new AuthzSessionExternalStateMock(&authManager)),
        client(),
        server(&authSession),
        mechanism("GSSAPI") {
    }

    void SaslConversationGssapi::assertConversationFailure() {
        std::string clientMessage;
        std::string serverMessage;
        Status clientStatus(ErrorCodes::InternalError, "");
        Status serverStatus(ErrorCodes::InternalError, "");
        do {
            clientStatus = client.step(serverMessage, &clientMessage);
            if (!clientStatus.isOK())
                break;
            serverStatus = server.step(clientMessage, &serverMessage);
            if (!serverStatus.isOK())
                break;
        } while (!client.isDone());
        ASSERT_FALSE(serverStatus.isOK() && clientStatus.isOK());
    }

    TEST_F(SaslConversationGssapi, SuccessfulAuthentication) {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, userName);
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        std::string clientMessage;
        std::string serverMessage;
        do {
            ASSERT_OK(client.step(serverMessage, &clientMessage));
            ASSERT_OK(server.step(clientMessage, &serverMessage));
        } while (!client.isDone());
        ASSERT_TRUE(server.isDone());
    }

    TEST_F(SaslConversationGssapi, NoSuchUser) {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, "WrongUserName");
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        assertConversationFailure();
    }

    TEST_F(SaslConversationGssapi, WrongServiceNameClient) {
        client.setParameter(SaslClientSession::parameterServiceName, "nosuch");
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, userName);
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        assertConversationFailure();
    }

    TEST_F(SaslConversationGssapi, WrongServerHostNameClient) {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, "badhost.10gen.me");
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, userName);
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

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
