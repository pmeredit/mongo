/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include <boost/scoped_ptr.hpp>
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
#include "mongo/db/auth/sasl_authentication_session.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/platform/unordered_map.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"

namespace mongo {

    int Command::testCommandsEnabled = 1; // To fix compile without needing to link Command code.

namespace {

    class SaslConversation : public unittest::Test {
    public:
        explicit SaslConversation(std::string mech); 

        void testSuccessfulAuthentication();
        void testNoSuchUser();
        void testBadPassword();
        void testWrongClientMechanism();
        void testWrongServerMechanism();

        AuthzManagerExternalStateMock* authManagerExternalState;
        AuthorizationManager authManager;
        AuthzSessionExternalStateMock* authzSessionExternalState;
        AuthorizationSession authSession;
        std::string mechanism;
        boost::scoped_ptr<SaslClientSession> client;
        boost::scoped_ptr<SaslAuthenticationSession> server;

    private:
        void assertConversationFailure();
    };

    class SaslIllegalConversation : public SaslConversation {
    public:
        SaslIllegalConversation():
            SaslConversation("ILLEGAL") {
        }
    };

    const std::string mockServiceName = "mocksvc";
    const std::string mockHostName = "host.mockery.com";

    SaslConversation::SaslConversation(std::string mech) :
        authManagerExternalState(new AuthzManagerExternalStateMock),
        authManager(authManagerExternalState),
        authzSessionExternalState(new AuthzSessionExternalStateMock(&authManager)),
        authSession(authzSessionExternalState),
        mechanism(mech) {

        OperationContextNoop txn;

        client.reset(SaslClientSession::create(mechanism));
        server.reset(SaslAuthenticationSession::create(&authSession, mechanism));
        ASSERT_OK(authManagerExternalState->updateOne(
                &txn,
                AuthorizationManager::versionCollectionNamespace,
                AuthorizationManager::versionDocumentQuery,
                BSON("$set" << BSON(AuthorizationManager::schemaVersionFieldName <<
                                    AuthorizationManager::schemaVersion26Final)),
                true,
                BSONObj()));
        ASSERT_OK(authManagerExternalState->insert(
                &txn,
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
            clientStatus = client->step(serverMessage, &clientMessage);
            if (!clientStatus.isOK())
                break;
            serverStatus = server->step(clientMessage, &serverMessage);
            if (!serverStatus.isOK())
                break;
        } while (!client->isDone());
        ASSERT_FALSE(serverStatus.isOK() &&
                     clientStatus.isOK() &&
                     client->isDone() &&
                     server->isDone());
    }

    void SaslConversation::testSuccessfulAuthentication() {
        client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client->setParameter(SaslClientSession::parameterMechanism, mechanism);
        client->setParameter(SaslClientSession::parameterUser, "andy");
        client->setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client->initialize());

        ASSERT_OK(server->start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        std::string clientMessage;
        std::string serverMessage;
        do {
            ASSERT_OK(client->step(serverMessage, &clientMessage));
            ASSERT_OK(server->step(clientMessage, &serverMessage));
        } while (!client->isDone());
        ASSERT_TRUE(server->isDone());
    }

    void SaslConversation::testNoSuchUser() {
        client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client->setParameter(SaslClientSession::parameterMechanism, mechanism);
        client->setParameter(SaslClientSession::parameterUser, "nobody");
        client->setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client->initialize());

        ASSERT_OK(server->start("test",
                               mechanism,
                               mockServiceName,
                               mockHostName,
                               1,
                               true));

        assertConversationFailure();
    }

    void SaslConversation::testBadPassword() {
        client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client->setParameter(SaslClientSession::parameterMechanism, mechanism);
        client->setParameter(SaslClientSession::parameterUser, "andy");
        client->setParameter(SaslClientSession::parameterPassword, "WRONG");
        ASSERT_OK(client->initialize());

        ASSERT_OK(server->start("test",
                                mechanism,
                                mockServiceName,
                                mockHostName,
                                1,
                                true));


        assertConversationFailure();
    }

    void SaslConversation::testWrongClientMechanism() {
        client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client->setParameter(SaslClientSession::parameterMechanism,
                            mechanism != "CRAM-MD5" ? "CRAM-MD5" : "PLAIN");
        client->setParameter(SaslClientSession::parameterUser, "andy");
        client->setParameter(SaslClientSession::parameterPassword, "frim");
        client->initialize();

        server->start("test",
                      mechanism,
                      mockServiceName,
                      mockHostName,
                      1,
                      true);

        assertConversationFailure();
    }

    void SaslConversation::testWrongServerMechanism() {
        client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client->setParameter(SaslClientSession::parameterMechanism, mechanism);
        client->setParameter(SaslClientSession::parameterUser, "andy");
        client->setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client->initialize());

        server->start("test",
                      mechanism != "CRAM-MD5" ? "CRAM-MD5" : "PLAIN",
                      mockServiceName,
                      mockHostName,
                      1,
                      true);
        assertConversationFailure();
    }

#define DEFINE_MECHANISM_FIXTURE(CLASS_SUFFIX, MECH_NAME)               \
    class SaslConversation##CLASS_SUFFIX : public SaslConversation {    \
    public:                                                             \
        SaslConversation##CLASS_SUFFIX():                               \
            SaslConversation(MECH_NAME) {                               \
        }                                                               \
    }                                                                   \

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
    TEST_MECHANISM(SCRAMSHA1, "SCRAM-SHA-1")
    TEST_MECHANISM(PLAIN, "PLAIN")

    TEST_F(SaslIllegalConversation, IllegalClientMechanism) {
        client->setParameter(SaslClientSession::parameterServiceName, mockServiceName);
        client->setParameter(SaslClientSession::parameterServiceHostname, mockHostName);
        client->setParameter(SaslClientSession::parameterMechanism, "FAKE");
        client->setParameter(SaslClientSession::parameterUser, "andy");
        client->setParameter(SaslClientSession::parameterPassword, "frim");
        ASSERT_OK(client->initialize());

        std::string clientMessage;
        std::string serverMessage;
        ASSERT_NOT_OK(client->step(serverMessage, &clientMessage));
    }

    TEST_F(SaslIllegalConversation, IllegalServerMechanism) {
        ASSERT_NOT_OK(server->start("test",
                                    "FAKE",
                                    mockServiceName,
                                    mockHostName,
                                    1,
                                    true));
    }

}  // namespace

}  // namespace mongo
