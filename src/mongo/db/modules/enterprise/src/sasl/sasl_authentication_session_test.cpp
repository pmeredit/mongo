/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include <string>
#include <vector>

#include "mongo/bson/mutable/algorithm.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/bson/mutable/element.h"
#include "mongo/client/sasl_client_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/jsobj.h"
#include "mongo/platform/unordered_map.h"
#include "mongo/unittest/unittest.h"
#include "sasl_authentication_session.h"

namespace mongo {

    int Command::testCommandsEnabled = 1; // To fix compile without needing to link Command code.

namespace {

    class SaslConversation : public unittest::Test {
    public:
        SaslConversation();

        void testSuccessfulAuthentication();
        void testNoSuchUser();
        void testBadPassword();
        void testWrongClientMechanism();
        void testWrongServerMechanism();

        AuthzManagerExternalStateMock* authManagerExternalState;
        AuthorizationManager authManager;
        AuthzSessionExternalStateMock* authzSessionExternalState;
        AuthorizationSession authSession;
        SaslClientSession client;
        SaslAuthenticationSession server;
        std::string mechanism;

    private:
        void assertConversationFailure();
    };

    const std::string mockServiceName = "mocksvc";
    const std::string mockHostName = "host.mockery.com";

    SaslConversation::SaslConversation() :
        authManagerExternalState(new AuthzManagerExternalStateMock),
        authManager(authManagerExternalState),
        authzSessionExternalState(new AuthzSessionExternalStateMock(&authManager)),
        authSession(authzSessionExternalState),
        client(),
        server(&authSession) {

        ASSERT_OK(authManagerExternalState->updateOne(
                AuthorizationManager::versionCollectionNamespace,
                AuthorizationManager::versionDocumentQuery,
                BSON("$set" << BSON(AuthorizationManager::schemaVersionFieldName <<
                                    AuthorizationManager::schemaVersion26Final)),
                true,
                BSONObj()));
        ASSERT_OK(authManagerExternalState->insert(
                NamespaceString("admin.system.users"),
                BSON("_id" << "test.andy" <<
                     "user" << "andy" <<
                     "db" << "test" <<
                     "credentials" << BSON("MONGODB-CR" << "frim") <<
                     "roles" << BSONArray()),
                BSONObj()));
    }

    void SaslConversation::assertConversationFailure() {
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
        ASSERT_FALSE(serverStatus.isOK() &&
                     clientStatus.isOK() &&
                     client.isDone() &&
                     server.isDone());
    }

    void SaslConversation::testSuccessfulAuthentication() {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, "andy");
        client.setParameter(SaslClientSession::parameterPassword, "frim");
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

    void SaslConversation::testNoSuchUser() {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, "nobody");
        client.setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        assertConversationFailure();
    }

    void SaslConversation::testBadPassword() {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, "andy");
        client.setParameter(SaslClientSession::parameterPassword, "WRONG");
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));


        assertConversationFailure();
    }

    void SaslConversation::testWrongClientMechanism() {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism,
                            mechanism == "CRAM-MD5" ? "PLAIN" : "CRAM-MD5");
        client.setParameter(SaslClientSession::parameterUser, "andy");
        client.setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        assertConversationFailure();
    }

    void SaslConversation::testWrongServerMechanism() {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, mechanism);
        client.setParameter(SaslClientSession::parameterUser, "andy");
        client.setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client.initialize());

        ASSERT_OK(server.start("test",
                               mechanism == "CRAM-MD5" ? "PLAIN" : "CRAM-MD5",
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        assertConversationFailure();
    }

#define DEFINE_MECHANISM_FIXTURE(CLASS_SUFFIX, MECH_NAME)               \
    class SaslConversation##CLASS_SUFFIX : public SaslConversation {    \
    public:                                                             \
        SaslConversation##CLASS_SUFFIX() { mechanism = MECH_NAME; }     \
    }

#define DEFINE_MECHANISM_TEST(FIXTURE_NAME, TEST_NAME)          \
    TEST_F(FIXTURE_NAME, TEST_NAME) { test##TEST_NAME(); }

#define DEFINE_ALL_MECHANISM_TESTS(FIXTURE_NAME)                  \
    DEFINE_MECHANISM_TEST(FIXTURE_NAME, SuccessfulAuthentication) \
    DEFINE_MECHANISM_TEST(FIXTURE_NAME, NoSuchUser)               \
    DEFINE_MECHANISM_TEST(FIXTURE_NAME, BadPassword)              \
    DEFINE_MECHANISM_TEST(FIXTURE_NAME, WrongClientMechanism)     \
    DEFINE_MECHANISM_TEST(FIXTURE_NAME, WrongServerMechanism)

#define TEST_MECHANISM(CLASS_SUFFIX, MECH_NAME) \
    DEFINE_MECHANISM_FIXTURE(CLASS_SUFFIX, MECH_NAME); \
    DEFINE_ALL_MECHANISM_TESTS(SaslConversation##CLASS_SUFFIX)

    TEST_MECHANISM(CRAMMD5, "CRAM-MD5")
    //TEST_MECHANISM(DIGESTMD5, "DIGEST-MD5")
    TEST_MECHANISM(PLAIN, "PLAIN")

    TEST_F(SaslConversation, IllegalClientMechanism) {
        client.setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client.setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client.setParameter(SaslClientSession::parameterMechanism, "FAKE");
        client.setParameter(SaslClientSession::parameterUser, "andy");
        client.setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client.initialize());

        std::string clientMessage;
        std::string serverMessage;
        ASSERT_NOT_OK(client.step(serverMessage, &clientMessage));
    }

    TEST_F(SaslConversation, IllegalServerMechanism) {
        ASSERT_NOT_OK(server.start("test",
                                   "FAKE",
                                   mockServiceName,
                                   mockHostName,
                                   1,
                                   true));
    }

}  // namespace

}  // namespace mongo
