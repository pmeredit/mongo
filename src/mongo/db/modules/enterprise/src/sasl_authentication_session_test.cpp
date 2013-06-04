/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include <string>
#include <vector>

#include "mongo/bson/mutable/algorithm.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/bson/mutable/element.h"
#include "mongo/client/sasl_client_session.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/jsobj.h"
#include "mongo/platform/unordered_map.h"
#include "mongo/unittest/unittest.h"
#include "sasl_authentication_session.h"

namespace mongo {
namespace {

    class AuthManagerExternalStateForSaslTesting : public AuthzManagerExternalStateMock {
    public:
        AuthManagerExternalStateForSaslTesting() {}

        virtual bool _findUser(const std::string& usersNamespace,
                               const BSONObj& query,
                               BSONObj* result) const;

        void addUserDocument(const std::string& usersNamespace, const BSONObj& userDocument);

    private:
        typedef std::vector<BSONObj> UsersCollection;
        typedef unordered_map<std::string, UsersCollection> UsersCollectionMap;
        UsersCollectionMap _usersCollections;
    };

    bool AuthManagerExternalStateForSaslTesting::_findUser(const std::string& usersNamespace,
                                                           const BSONObj& queryRaw,
                                                           BSONObj* result) const {

        // TODO: User the matcher instead, when it can be plugged into a unit test.
        namespace mmb = mutablebson;
        const mmb::Document query(queryRaw);
        mmb::ConstElement userNameElement = mmb::findFirstChildNamed(query.root(), "user");
        mmb::ConstElement userSourceElement = mmb::findFirstChildNamed(query.root(), "userSource");
        const size_t numQueryFields = mmb::countChildren(query.root());

        ASSERT_GREATER_THAN(numQueryFields, 0U);
        ASSERT_LESS_THAN_OR_EQUALS(numQueryFields, 2U);
        ASSERT_TRUE(userNameElement.ok());
        ASSERT_EQUALS(String, userNameElement.getType());

        if (numQueryFields == 2) {
            ASSERT_TRUE(userSourceElement.ok());
            if (!userSourceElement.isValueNull()) {
                ASSERT_EQUALS(String, userSourceElement.getType());
            }
        }

        const UsersCollectionMap::const_iterator coll = _usersCollections.find(usersNamespace);
        if (coll == _usersCollections.end())
            return false;
        UsersCollection::const_iterator doc;
        for (doc = coll->second.begin(); doc != coll->second.end(); ++doc) {
            // "user" field must match.
            mmb::Document mdoc(*doc);
            if (0 != mmb::findFirstChildNamed(mdoc.root(),
                                              "user").compareWithElement(userNameElement)) {
                continue;
            }

            // If query did not specify a userSource field, we're done!
            if (!userSourceElement.ok()) {
                break;  // Found it!
            }
            mmb::ConstElement docUserSourceElement = mmb::findFirstChildNamed(mdoc.root(),
                                                                              "userSource");

            // If the userSourceElement is explicitly null, then "userSource" must be missing or
            // null in doc to match.
            if (userSourceElement.isValueNull()) {
                if (!docUserSourceElement.ok() || docUserSourceElement.isValueNull()) {
                    break;
                }
                else {
                    continue;
                }
            }

            // userSource in query not null; must be a String, which must equal the string in doc to
            // match.
            ASSERT_EQUALS(String, userSourceElement.getType());
            StringData docUserSourceValue;
            if (!docUserSourceElement.ok() || docUserSourceElement.isValueNull()) {
                continue;
            }
            if (docUserSourceElement.getValueString() == userSourceElement.getValueString()) {
                // Match!
                break;
            }
        }
        if (doc == coll->second.end()) {
            return false;
        }
        *result = *doc;
        return true;
    }

    void AuthManagerExternalStateForSaslTesting::addUserDocument(const std::string& usersNamespace,
                                                                 const BSONObj& userDocument) {
        _usersCollections[usersNamespace].push_back(userDocument.getOwned());
    }

    class SaslConversation : public unittest::Test {
    public:
        SaslConversation();

        void testSuccessfulAuthentication();
        void testNoSuchUser();
        void testBadPassword();
        void testWrongClientMechanism();
        void testWrongServerMechanism();

        AuthManagerExternalStateForSaslTesting* authManagerExternalState;
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
        authManagerExternalState(new AuthManagerExternalStateForSaslTesting()),
        authManager(authManagerExternalState),
        authzSessionExternalState(new AuthzSessionExternalStateMock(&authManager)),
        authSession(authzSessionExternalState),
        client(),
        server(&authSession) {

        authManagerExternalState->addUserDocument("test.system.users",
                                                  BSON("user" << "andy" << "pwd" << "frim"));
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
