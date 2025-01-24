/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_deduplication.h"
#include "audit/audit_log.h"
#include "audit/audit_manager.h"
#include "audit/logger/mock_appender.h"
#include "audit/mongo/audit_mongo.h"
#include "audit/ocsf/audit_ocsf.h"
#include "audit/ocsf/ocsf_constants.h"

#include "mongo/base/data_range.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/audit.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/role_name.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/change_stream_options_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/database_name.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/member_state.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_test_fixture.h"
#include "mongo/rpc/metadata/audit_client_attrs.h"
#include "mongo/transport/mock_session.h"
#include "mongo/transport/transport_layer_mock.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/options_parser/environment.h"
#include "mongo/util/processinfo.h"


#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

namespace moe = mongo::optionenvironment;

namespace mongo::audit {

namespace {

constexpr auto COLLECTION_NAME = "coll"_sd;
constexpr auto COMMAND_NAME = "command"_sd;
constexpr auto DB_NAME = "test"_sd;
constexpr auto ROLE_NAME = "role"_sd;
constexpr auto USER_NAME = "user"_sd;
constexpr auto VIEW_NAME = "view"_sd;
constexpr auto PRIVILEGE_DB_NAME = "admin"_sd;
constexpr auto PRIVILEGE_COLLECTION_NAME = "system.users"_sd;

const RoleName kRoleName(ROLE_NAME, DB_NAME);
const UserName kUserName(USER_NAME, DB_NAME);

const DatabaseName kDatabaseName = DatabaseName::createDatabaseName_forTest(boost::none, DB_NAME);
const AuthenticateEvent kAuthEvent(
    "SCRAM-SHA256", kUserName, [](BSONObjBuilder*) {}, ErrorCodes::OK);

const NamespaceString kNamespaceString(
    NamespaceString::createNamespaceString_forTest(DB_NAME, COLLECTION_NAME));
const NamespaceString kNamespaceStringAlt(
    NamespaceString::createNamespaceString_forTest(DB_NAME, VIEW_NAME));
const NamespaceString kNamespaceStringPrivilege(
    NamespaceString::createNamespaceString_forTest(PRIVILEGE_DB_NAME, PRIVILEGE_COLLECTION_NAME));

const std::vector<RoleName> kRolesVector = {kRoleName};

const BSONObj kCustomData{};

const PrivilegeVector kPrivilegeVector({Privilege(
    ResourcePattern::forAnyNormalResource(boost::none), ActionType::refineCollectionShardKey)});

const auto kOldReplsetConfig = BSON("_id"
                                    << "ABCD"
                                    << "members"
                                    << BSONArray(BSON("0"
                                                      << "a")));
const auto kNewReplsetConfig = BSON("_id"
                                    << "ABCD"
                                    << "members"
                                    << BSONArray(BSON("0"
                                                      << "a"
                                                      << "1"
                                                      << "b")));

const auto kUsersBefore = BSONArray(BSON("0" << BSON("user"
                                                     << "john"
                                                     << "database" << DB_NAME)));
const auto kUsersAfter = BSONArray();

const auto kFooDoc = BSON("foo" << 1);

BSONObj getLastNormalized() {
    // We round trip through JSON to take advantage of the lossy transcoding
    // as a means to normalize longs to int and similar.
    return fromjson(getLastLine_forTest().jsonString(JsonStringFormat::LegacyStrict));
}

class CommandInterfaceMock : public audit::CommandInterface {
public:
    std::set<StringData> sensitiveFieldNames() const override {
        return {};
    }

    void snipForLogging(mutablebson::Document* cmdObj) const override{};

    StringData getName() const override {
        return COMMAND_NAME;
    }

    NamespaceString ns() const override {
        return kNamespaceString;
    }

    bool redactArgs() const override {
        return false;
    }
};

/**
 * This function iterates over all the values in matchFields and asserts
 * that they exist in the log object.
 */
void checkPartialLogLine(BSONObj& log, const BSONObj& matchFields) {
    LOGV2(9792000,
          "Asserting fields from log have all fields from matchFields.",
          "log"_attr = log,
          "matchFields"_attr = matchFields);

    for (const auto field : matchFields) {
        if (!log.hasField(field.fieldNameStringData())) {
            LOGV2(8307601,
                  "Failed partial log line match, Field name not found.",
                  "fieldname"_attr = field.fieldNameStringData());
        }
        ASSERT(log.hasField(field.fieldNameStringData()));

        const auto logElem = log.getField(field.fieldNameStringData());
        if (logElem.toString() != field.toString()) {
            LOGV2(8307602,
                  "Log element values do not match.",
                  "firstElem"_attr = logElem,
                  "secondElem"_attr = field);
        }

        ASSERT(logElem.toString() == field.toString());
    }
}

template <typename Schema>
class AuditTest : public ServiceContextTest {
public:
    void setUpAuditing() {
        auto* am = getGlobalAuditManager();
        if (am->isEnabled()) {
            return;
        }

        moe::Environment env;
        ASSERT_OK(env.set(moe::Key("auditLog.destination"), moe::Value("mock")));
        getGlobalAuditManager()->initialize(env);

        // Validate initial state.
        ASSERT_EQ(am->getAuditAuthorizationSuccess(), false);
    }

    Schema* getAudit() const {
        return checked_cast<Schema*>(AuditInterface::get(getServiceContext()));
    }

    void setUp() override {
        ServiceContextTest::setUp();
        setUpAuditing();

        AuditInterface::set(getServiceContext(), std::make_unique<Schema>());

        // Setting a client with a mock session to get more realistic mock data.
        session = transport::MockSession::create(&transportLayer);
        Client::releaseCurrent();
        Client::setCurrent(getService()->makeClient("session", session));
        opCtx = makeOperationContext();

        rpc::AuditClientAttrs attrs{HostAndPort(), HostAndPort()};
        rpc::AuditClientAttrs::set(Client::getCurrent(), attrs);

        // Ensure we are not a replica set so we can get DDL auditing by
        // creating a default ReplSettings.
        repl::ReplicationCoordinator::set(getServiceContext(),
                                          std::make_unique<repl::ReplicationCoordinatorMock>(
                                              getServiceContext(), repl::ReplSettings()));
    }

    void tearDown() final {
        getGlobalAuditManager()->~AuditManager();
        new (getGlobalAuditManager()) AuditManager();
        opCtx.reset();
        AuditInterface::set(getServiceContext(), nullptr);
    }

    std::shared_ptr<transport::MockSession> session;
    transport::TransportLayerMock transportLayer;
    ServiceContext::UniqueOperationContext opCtx;
};

class AuditMongoTest : public AuditTest<AuditMongo> {};

class AuditOCSFTest : public AuditTest<AuditOCSF> {
public:
    constexpr static StringData kTypeIDField = "type_id"_sd;
    constexpr static StringData kNameField = "name"_sd;
    constexpr static StringData kUserField = "user"_sd;
    constexpr static StringData kGroupsField = "groups"_sd;
    constexpr static StringData kEntityField = "entity"_sd;
    constexpr static StringData kTypeField = "type"_sd;
    constexpr static StringData kDataField = "data"_sd;
    constexpr static StringData kProcessIdField = "pid"_sd;
    constexpr static StringData kOSField = "os"_sd;
    constexpr static int kTypeIdUnknown = 0;
    constexpr static int kTypeIdWindows = 100;
    constexpr static int kTypeIdLinux = 200;
    constexpr static int kTypeIdMacOS = 300;
};

/**
 * Test logDirectAuthOperation for Mongo and OCSF.
 */
TEST_F(AuditMongoTest, testLogDirectAuthOperationNSSMongo) {
    // If NSS is a privilege collection, we should not audit.
    auto auditMongo = getAudit();
    BSONObj log = getLastNormalized();
    auditMongo->logDirectAuthOperation(
        getClient(), kNamespaceString, kFooDoc, DirectAuthOperation::kInsert);
    ASSERT_BSONOBJ_EQ(log, getLastNormalized());
}

TEST_F(AuditOCSFTest, testLogDirectAuthOperationNSSOCSF) {
    // If NSS is a privilege collection, we should not audit.
    auto auditOCSF = getAudit();
    BSONObj log = getLastNormalized();
    auditOCSF->logDirectAuthOperation(
        getClient(), kNamespaceString, kFooDoc, DirectAuthOperation::kInsert);
    ASSERT_BSONOBJ_EQ(log, getLastNormalized());
}

TEST_F(AuditMongoTest, testLogDirectAuthOperationAlreadyAuditedMongo) {
    // If operation was already audited, we should not audit again.
    auto auditMongo = getAudit();
    AuditDeduplicationMongo::markOperationAsAudited(getClient());
    BSONObj log = getLastNormalized();
    auditMongo->logDirectAuthOperation(
        getClient(), kNamespaceString, kFooDoc, DirectAuthOperation::kInsert);
    ASSERT_BSONOBJ_EQ(log, getLastNormalized());
}

TEST_F(AuditOCSFTest, testLogDirectAuthOperationAlreadyAuditedOCSF) {
    // If operation was already audited, we should not audit again.
    auto auditOCSF = getAudit();
    AuditDeduplicationOCSF::markOperationAsAudited(getClient());
    BSONObj log = getLastNormalized();
    auditOCSF->logDirectAuthOperation(
        getClient(), kNamespaceString, kFooDoc, DirectAuthOperation::kInsert);
    ASSERT_BSONOBJ_EQ(log, getLastNormalized());
}


/**
 * Test isDDLAuditingAllowed function.
 */
TEST_F(AuditMongoTest, testIsDDLAuditingAllowed) {
    // If we unset the replication coordinator, DDL auditing should
    // not work.
    repl::ReplicationCoordinator::set(getServiceContext(), nullptr);
    ASSERT_FALSE(isDDLAuditingAllowed(getClient(), kNamespaceString));


    // If we set the default replication coordinator, our replication member
    // state will be unknown and DDLCoordination will not be allowed.
    repl::ReplicationCoordinator::set(
        getServiceContext(),
        std::make_unique<repl::ReplicationCoordinatorMock>(getServiceContext()));
    ASSERT_FALSE(isDDLAuditingAllowed(getClient(), kNamespaceString));

    // If we transition our member state to primary, we should be able to
    // perform DDL operations.
    ASSERT(repl::ReplicationCoordinator::get(getServiceContext())
               ->setFollowerMode(repl::MemberState::RS_PRIMARY)
               .isOK());
    ASSERT_TRUE(isDDLAuditingAllowed(getClient(), kNamespaceString));
}

/**
 * Test AuditOCSF::AuditEventOCSF::_buildNetwork.
 */
TEST_F(AuditOCSFTest, auditOCSFBuildNetwork) {
    // Client does not have a session, should be an
    // empty object.
    {
        BSONObjBuilder builder;
        auto cl = getServiceContext()->getService()->makeClient("test", nullptr);
        AuditOCSF::AuditEventOCSF::_buildNetwork(cl.get(), &builder);
        auto obj = builder.obj();
        ASSERT(obj.isEmpty());
    }

    // Adding a mock session.
    {
        BSONObjBuilder builder;
        AuditOCSF::AuditEventOCSF::_buildNetwork(getClient(), &builder);
        auto obj = builder.obj();
        checkPartialLogLine(
            obj,
            fromjson("{ src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: { "
                     "interface_name: 'unix', ip: 'anonymous' } }"));
    }
}

/**
 * Test AuditOCSF::AuditEventOCSF::_buildUser.
 */
TEST_F(AuditOCSFTest, auditOCSFBuildUser) {
    auto stripUser = [](const BSONObj& obj) -> BSONObj {
        ASSERT(obj.hasField(AuditOCSFTest::kUserField));
        return obj.getObjectField(AuditOCSFTest::kUserField);
    };

    auto validateUsername = [](const BSONObj& obj) {
        ASSERT(obj.hasField(AuditOCSFTest::kTypeIDField));
        ASSERT(obj.getIntField(AuditOCSFTest::kTypeIDField) == ocsf::kUserTypeIdRegularUser);

        ASSERT(obj.hasField(AuditOCSFTest::kNameField));
        ASSERT(obj.getStringField(AuditOCSFTest::kNameField) == kUserName.getUnambiguousName());
    };

    // Build basic user from just username.
    {
        BSONObjBuilder builder;
        AuditOCSF::AuditEventOCSF::_buildUser(&builder, kUserName);

        auto obj = builder.obj();
        validateUsername(stripUser(obj));
    }

    // Build basic user from just username.
    {
        BSONObjBuilder builder;
        AuditOCSF::AuditEventOCSF::_buildUser(&builder, kUserName, kRolesVector);

        auto obj = builder.obj();

        auto nested = stripUser(obj);
        validateUsername(nested);

        ASSERT(nested.hasField(AuditOCSFTest::kGroupsField));
        auto roles = BSONArray(nested.getObjectField(AuditOCSFTest::kGroupsField));

        ASSERT(roles.nFields() == 1);
        auto firstRole = roles.firstElement().Obj().firstElement();

        ASSERT(firstRole.fieldName() == AuditOCSFTest::kNameField);
        ASSERT(firstRole.String() == kRoleName.getUnambiguousName());
    }
}

/**
 * Test AuditOCSF::AuditEventOCSF::_buildProcess.
 */

TEST_F(AuditOCSFTest, auditOCSFBuildProcess) {
    {
        BSONObjBuilder builder;
        AuditOCSF::AuditEventOCSF::_buildProcess(&builder);
        auto obj = builder.obj();

        auto pid = ProcessId::getCurrent().asInt64();
        ASSERT(obj.hasField(AuditOCSFTest::kProcessIdField));
        ASSERT(obj.getIntField(AuditOCSFTest::kProcessIdField) == pid);
    }
}

/**
 * Test AuditOCSF::AuditEventOCSF::_buildEntity.
 */

TEST_F(AuditOCSFTest, auditOCSFBuildEntity) {
    BSONObj data = BSON("foo" << 1);

    {
        BSONObjBuilder builder;
        AuditOCSF::AuditEventOCSF::_buildEntity(
            &builder, kEntityField, &data, ROLE_NAME, USER_NAME);

        auto obj = builder.obj();
        ASSERT(obj.hasField(AuditOCSFTest::kEntityField));
        auto nested = obj.getObjectField(AuditOCSFTest::kEntityField);

        ASSERT(nested.hasField(AuditOCSFTest::kNameField));
        ASSERT(nested.getStringField(AuditOCSFTest::kNameField) == ROLE_NAME);

        ASSERT(nested.hasField(AuditOCSFTest::kTypeField));
        ASSERT(nested.getStringField(AuditOCSFTest::kTypeField) == USER_NAME);

        ASSERT(nested.hasField(AuditOCSFTest::kDataField));
        ASSERT_BSONOBJ_EQ(nested.getObjectField(AuditOCSFTest::kDataField), data);
    }
}

/**
 * Test AuditOCSF::AuditEventOCSF::_buildDevice.
 */

TEST_F(AuditOCSFTest, auditOCSFBuildDevice) {
    {
        BSONObjBuilder builder;
        AuditOCSF::AuditEventOCSF::_buildDevice(&builder);

        auto obj = builder.obj();

        ASSERT(obj.hasField(AuditOCSFTest::kTypeIDField));
        ASSERT(obj.getIntField(AuditOCSFTest::kTypeIDField) == ocsf::kUserTypeIdUnknown);

        ASSERT(obj.hasField(AuditOCSFTest::kOSField));
        auto nested = obj.getObjectField(AuditOCSFTest::kOSField);

        const auto& osType = ProcessInfo::getOsType();
        ASSERT(nested.hasField(AuditOCSFTest::kTypeIDField));
        if (osType == "linux") {
            ASSERT(nested.getIntField(AuditOCSFTest::kTypeIDField) == AuditOCSFTest::kTypeIdLinux);
        } else if (osType == "windows") {
            ASSERT(nested.getIntField(AuditOCSFTest::kTypeIDField) ==
                   AuditOCSFTest::kTypeIdWindows);
        } else if (osType == "macos") {
            ASSERT(nested.getIntField(AuditOCSFTest::kTypeIDField) == AuditOCSFTest::kTypeIdMacOS);
        } else {
            ASSERT(nested.getIntField(AuditOCSFTest::kTypeIDField) ==
                   AuditOCSFTest::kTypeIdUnknown);
        }
    }
}

/**
 * The tests below (basicLogClientMetadata to basicLogRotateLog) do a cursory test for all
 * the different types of log lines. They are more thoroughly covered by our integration tests
 * but the tests here allow us better access to debugging.
 */
TEST_F(AuditMongoTest, basicLogClientMetadataMongo) {
    logClientMetadata(getClient());
    auto expectedOutputMongo =
        "{ atype: 'clientMetadata', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: {}, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogClientMetadataOCSF) {
    logClientMetadata(getClient());
    auto expectedOutputOCSF =
        "{ activity_id: 99, category_uid: 4, class_uid: 4001, severity_id: 1, type_uid: "
        "400199, actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, "
        "dst_endpoint: { interface_name: 'unix', ip: 'anonymous' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogAuthenticationMongo) {
    logAuthentication(getClient(), kAuthEvent);
    auto expectedOutputMongo =
        "{ atype: 'authenticate', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { user: 'user', db: 'test', mechanism: 'SCRAM-SHA256' }, "
        "result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogAuthenticationOCSF) {
    logAuthentication(getClient(), kAuthEvent);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3002, severity_id: 1, type_uid: 300201, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, user: { type_id: 1, name: 'test.user' }, "
        "auth_protocol: 'SCRAM-SHA256', unmapped: { atype: 'authenticate' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}


TEST_F(AuditMongoTest, basicLogCommandAuthzCheckMongo) {
    logCommandAuthzCheck(getClient(), {}, CommandInterfaceMock(), ErrorCodes::BadValue);
    auto expectedOutputMongo =
        "{ atype: 'authCheck', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { command: 'command', ns: 'test.coll', args: {} }, result: 2 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogCommandAuthzCheckOCSF) {
    logCommandAuthzCheck(getClient(), {}, CommandInterfaceMock(), ErrorCodes::BadValue);
    auto expectedOutputOCSF =
        "{ activity_id: 0, category_uid: 6, class_uid: 6003, severity_id: 1, type_uid: 600300, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, api: { operation: 'command', request: { uid: "
        "'test.coll' }, response: { code: 2, error: 'BadValue' } }, unmapped: { args: {} } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogKillCursorsAuthzCheckMongo) {
    logKillCursorsAuthzCheck(getClient(), kNamespaceString, 100ll, ErrorCodes::BadValue);
    auto expectedOutputMongo =
        "{ atype: 'authCheck', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { command: 'killCursors', ns: 'test.coll', args: { killCursors: "
        "'coll', cursorId: 100 } }, result: 2 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogKillCursorsAuthzCheckOCSF) {
    logKillCursorsAuthzCheck(getClient(), kNamespaceString, 100ll, ErrorCodes::BadValue);
    auto expectedOutputOCSF =
        "{ activity_id: 99, category_uid: 6, class_uid: 6003, severity_id: 1, type_uid: "
        "600399, actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, "
        "dst_endpoint: { interface_name: 'unix', ip: 'anonymous' }, api: { operation: "
        "'killCursors', request: { uid: 'test.coll' }, response: { code: 2, error: 'BadValue' } }, "
        "unmapped: { args: { killCursors: 'coll', cursorId: 100 } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogCreateUserMongo) {
    logCreateUser(getClient(), kUserName, false, &kCustomData, kRolesVector, boost::none);
    auto expectedOutputMongo =
        "{ atype: 'createUser', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { user: 'user', db: 'test', customData: {}, roles: [ { role: "
        "'role', db: 'test' } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
    ASSERT(AuditDeduplicationMongo::wasOperationAlreadyAudited(getClient()));
}

TEST_F(AuditOCSFTest, basicLogCreateUserOCSF) {
    logCreateUser(getClient(), kUserName, false, &kCustomData, kRolesVector, boost::none);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300101, "
        "actor: {}, user: { type_id: 1, name: 'test.user', groups: [ { name: 'test.role' } ] "
        "}, unmapped: { atype: 'createUser', customData: {} } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
    ASSERT(AuditDeduplicationOCSF::wasOperationAlreadyAudited(getClient()));
}

TEST_F(AuditMongoTest, basicLogDropUserMongo) {
    logDropUser(getClient(), kUserName);
    auto expectedOutputMongo =
        "{ atype: 'dropUser', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { user: 'user', db: 'test' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropUserOCSF) {
    logDropUser(getClient(), kUserName);
    auto expectedOutputOCSF =
        "{ activity_id: 6, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300106, "
        "actor: {}, user: { type_id: 1, name: 'test.user' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogDropAllUsersFromDatabaseMongo) {
    logDropAllUsersFromDatabase(getClient(), kDatabaseName);
    auto expectedOutputMongo =
        "{ atype: 'dropAllUsersFromDatabase', local: { unix: 'anonymous' }, remote: { unix: "
        "'anonymous' }, users: [], roles: [], param: { db: 'test' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropAllUsersFromDatabaseOCSF) {
    logDropAllUsersFromDatabase(getClient(), kDatabaseName);
    auto expectedOutputOCSF =
        "{ activity_id: 6, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300106, "
        "actor: {}, unmapped: { atype: 'dropAllUsersFromDatabase', allUsersFromDatabase: "
        "'test' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogUpdateUserMongo) {
    logUpdateUser(getClient(), kUserName, false, {}, &kRolesVector, boost::none);
    auto expectedOutputMongo =
        "{ atype: 'updateUser', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { user: 'user', db: 'test', passwordChanged: false, roles: [ "
        "{ role: 'role', db: 'test' } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogUpdateUserOCSF) {
    logUpdateUser(getClient(), kUserName, false, {}, &kRolesVector, boost::none);
    auto expectedOutputOCSF =
        "{ activity_id: 99, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: "
        "300199, actor: {}, user: { type_id: 1, name: 'test.user', groups: [ { name: "
        "'test.role' } ] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogGrantRolesToUserMongo) {
    logGrantRolesToUser(getClient(), kUserName, kRolesVector);
    auto expectedOutputMongo =
        "{ atype: 'grantRolesToUser', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { user: 'user', db: 'test', roles: [ { role: 'role', db: "
        "'test' } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogGrantRolesToUserOCSF) {
    logGrantRolesToUser(getClient(), kUserName, kRolesVector);
    auto expectedOutputOCSF =
        "{ activity_id: 7, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300107, "
        "actor: {}, user: { type_id: 1, name: 'test.user' }, unmapped: { atype: "
        "'grantRolesToUser', roles: [ 'test.role' ] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRevokeRolesFromUserMongo) {
    logRevokeRolesFromUser(getClient(), kUserName, kRolesVector);
    auto expectedOutputMongo =
        "{ atype: 'revokeRolesFromUser', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { user: 'user', db: 'test', roles: [ { role: 'role', db: "
        "'test' } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogRevokeRolesFromUserOCSF) {
    logRevokeRolesFromUser(getClient(), kUserName, kRolesVector);
    auto expectedOutputOCSF =
        "{ activity_id: 8, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300108, "
        "actor: {}, user: { type_id: 1, name: 'test.user' }, unmapped: { atype: "
        "'revokeRolesFromUser', roles: [ 'test.role' ] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogCreateRoleMongo) {
    logCreateRole(getClient(), kRoleName, {}, kPrivilegeVector, boost::none);
    auto expectedOutputMongo =
        "{ atype: 'createRole', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { role: 'role', db: 'test', roles: [], privileges: [ { "
        "resource: { db: '', collection: '' }, actions: [ 'refineCollectionShardKey' ] } ] }, "
        "result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogCreateRoleOCSF) {
    logCreateRole(getClient(), kRoleName, {}, kPrivilegeVector, boost::none);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300101, "
        "actor: {}, unmapped: { atype: 'createRole', role: 'test.role', roles: [], privileges: "
        "[ { resource: { db: '', collection: '' }, actions: [ 'refineCollectionShardKey' ] } ] "
        "} }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogUpdateRoleMongo) {
    logUpdateRole(getClient(), kRoleName, &kRolesVector, &kPrivilegeVector, boost::none);
    auto expectedOutputMongo =
        "{ atype: 'updateRole', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { role: 'role', db: 'test', roles: [ { role: 'role', db: "
        "'test' } ], privileges: [ { resource: { db: '', collection: '' }, actions: [ "
        "'refineCollectionShardKey' ] } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogUpdateRoleOCSF) {
    logUpdateRole(getClient(), kRoleName, &kRolesVector, &kPrivilegeVector, boost::none);
    auto expectedOutputOCSF =
        "{ activity_id: 99, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: "
        "300199, actor: {}, unmapped: { atype: 'updateRole', role: 'test.role', roles: [ "
        "'test.role' ], privileges: [ { resource: { db: '', collection: '' }, actions: [ "
        "'refineCollectionShardKey' ] } ] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogDropRoleMongo) {
    logDropRole(getClient(), kRoleName);
    auto expectedOutputMongo =
        "{ atype: 'dropRole', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { role: 'role', db: 'test' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropRoleOCSF) {
    logDropRole(getClient(), kRoleName);
    auto expectedOutputOCSF =
        "{ activity_id: 6, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300106, "
        "actor: {}, unmapped: { atype: 'dropRole', role: 'test.role' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogDropAllRolesFromDatabaseMongo) {
    logDropAllRolesFromDatabase(getClient(), kDatabaseName);
    auto expectedOutputMongo =
        "{ atype: 'dropAllRolesFromDatabase', local: { unix: 'anonymous' }, remote: { unix: "
        "'anonymous' }, users: [], roles: [], param: { db: 'test' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropAllRolesFromDatabaseOCSF) {
    logDropAllRolesFromDatabase(getClient(), kDatabaseName);
    auto expectedOutputOCSF =
        "{ activity_id: 6, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300106, "
        "actor: {}, unmapped: { atype: 'dropAllRolesFromDatabase', db: 'test' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogGrantRolesToRoleMongo) {
    logGrantRolesToRole(getClient(), kRoleName, kRolesVector);
    auto expectedOutputMongo =
        "{ atype: 'grantRolesToRole', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { role: 'role', db: 'test', roles: [ { role: 'role', db: "
        "'test' } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogGrantRolesToRoleOCSF) {
    logGrantRolesToRole(getClient(), kRoleName, kRolesVector);
    auto expectedOutputOCSF =
        "{ activity_id: 7, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300107, "
        "actor: {}, unmapped: { atype: 'grantRolesToRole', role: 'test.role', roles: [ "
        "'test.role' ] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRevokeRolesFromRoleMongo) {
    logRevokeRolesFromRole(getClient(), kRoleName, kRolesVector);
    auto expectedOutputMongo =
        "{ atype: 'revokeRolesFromRole', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { role: 'role', db: 'test', roles: [ { role: 'role', db: "
        "'test' } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogRevokeRolesFromRoleOCSF) {
    logRevokeRolesFromRole(getClient(), kRoleName, kRolesVector);
    auto expectedOutputOCSF =
        "{ activity_id: 8, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300108, "
        "actor: {}, unmapped: { atype: 'revokeRolesFromRole', role: 'test.role', roles: [ "
        "'test.role' ] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogGrantPrivilegesToRoleMongo) {
    logGrantPrivilegesToRole(getClient(), kRoleName, kPrivilegeVector);
    auto expectedOutputMongo =
        "{ atype: 'grantPrivilegesToRole', local: { unix: 'anonymous' }, remote: { unix: "
        "'anonymous' }, users: [], roles: [], param: { role: 'role', db: 'test', privileges: [ { "
        "resource: { db: '', collection: '' }, actions: [ 'refineCollectionShardKey' ] } ] }, "
        "result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogGrantPrivilegesToRoleOCSF) {
    logGrantPrivilegesToRole(getClient(), kRoleName, kPrivilegeVector);
    auto expectedOutputOCSF =
        "{ activity_id: 7, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300107, "
        "actor: {}, unmapped: { atype: 'grantPrivilegesToRole', role: 'test.role', privileges: "
        "[ { resource: { db: '', collection: '' }, actions: [ 'refineCollectionShardKey' ] } ] "
        "} }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRevokePrivilegesFromRoleMongo) {
    logRevokePrivilegesFromRole(getClient(), kRoleName, kPrivilegeVector);
    auto expectedOutputMongo =
        "{ atype: 'revokePrivilegesFromRole', local: { unix: 'anonymous' }, remote: { unix: "
        "'anonymous' }, users: [], roles: [], param: { role: 'role', db: 'test', privileges: [ { "
        "resource: { db: '', collection: '' }, actions: [ 'refineCollectionShardKey' ] } ] }, "
        "result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogRevokePrivilegesFromRoleOCSF) {
    logRevokePrivilegesFromRole(getClient(), kRoleName, kPrivilegeVector);
    auto expectedOutputOCSF =
        "{ activity_id: 8, category_uid: 3, class_uid: 3001, severity_id: 1, type_uid: 300108, "
        "actor: {}, unmapped: { atype: 'revokePrivilegesFromRole', role: 'test.role', "
        "privileges: [ { resource: { db: '', collection: '' }, actions: [ "
        "'refineCollectionShardKey' ] } ] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogReplSetReconfigMongo) {
    logReplSetReconfig(getClient(), &kOldReplsetConfig, &kNewReplsetConfig);
    auto expectedOutputMongo =
        "{ atype: 'replSetReconfig', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { old: { _id: 'ABCD', members: [ 'a' ] }, new: { _id: "
        "'ABCD', members: [ 'a', 'b' ] } }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogReplSetReconfigOCSF) {
    logReplSetReconfig(getClient(), &kOldReplsetConfig, &kNewReplsetConfig);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 4, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'replSetReconfig', old: { _id: 'ABCD', members: [ 'a' ] "
        "}, new: { _id: 'ABCD', members: [ 'a', 'b' ] } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogApplicationMessageMongo) {
    logApplicationMessage(getClient(), "Test Application Message.");
    auto expectedOutputMongo =
        "{ atype: 'applicationMessage', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { msg: 'Test Application Message.' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogApplicationMessageOCSF) {
    logApplicationMessage(getClient(), "Test Application Message.");
    auto expectedOutputOCSF =
        "{ activity_id: 99, category_uid: 1, class_uid: 1007, severity_id: 1, type_uid: "
        "100799, actor: {}, unmapped: { atype: 'applicationMessage', msg: 'Test Application "
        "Message.' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogStartupOptionsMongo) {
    // LogStartupOptions requires all the clusterParameterOptions, so we need
    // to create this decoration on the serviceContext. Since this happens in mongod/s
    // main, we need to replicate here.
    ChangeStreamOptionsManager::create(getServiceContext());

    logStartupOptions(getClient(), moe::Environment().toBSON());
    auto expectedOutputMongo =
        "{ atype: 'startup', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { startupOptions: {}, initialClusterServerParameters: [ { _id: "
        "'addOrRemoveShardInProgress', clusterParameterTime: Timestamp(0, 0), inProgress: false }, "
        "{ _id: 'changeStreamOptions', clusterParameterTime: Timestamp(0, 0), preAndPostImages: { "
        "expireAfterSeconds: 'off' } }, { _id: 'changeStreams', clusterParameterTime: Timestamp(0, "
        "0), expireAfterSeconds: 3600 } , { _id: 'configServerReadPreferenceForCatalogQueries', "
        "clusterParameterTime: Timestamp(0, 0), mustAlwaysUseNearest: false }, { _id: "
        "'cwspTestNeedsFeatureFlagBlender', clusterParameterTime: Timestamp(0, 0), intData: 0 }, { "
        "_id: 'cwspTestNeedsLatestFCV', clusterParameterTime: Timestamp(0, 0), intData: 0 }, {_id: "
        "'defaultMaxTimeMS', clusterParameterTime: Timestamp(0, 0), readOperations: 0}, { _id: "
        "'fleCompactionOptions', clusterParameterTime: Timestamp(0, 0), maxCompactionSize: "
        "268435456, maxAnchorCompactionSize: 268435456, maxESCEntriesPerCompactionDelete: 350000 "
        "}, { _id: 'internalQueryCutoffForSampleFromRandomCursor', clusterParameterTime: "
        "Timestamp(0, 0), sampleCutoff: 0.05 }, "
        "{ '_id': 'internalSearchOptions', 'clusterParameterTime': { '$timestamp': { 't': 0,'i': 0 "
        "} }, 'oversubscriptionFactor': 1.064, 'batchSizeGrowthFactor' : 1.5 }, "
        "{ _id: 'pauseMigrationsDuringMultiUpdates', "
        "clusterParameterTime: Timestamp(0, 0), enabled: false }, {_id: 'querySettings', "
        "settingsArray: [], clusterParameterTime: Timestamp(0, 0)}, { _id: "
        "'shardedClusterCardinalityForDirectConns', clusterParameterTime: Timestamp(0, 0), "
        "hasTwoOrMoreShards: false }, { _id: 'testBoolClusterParameter', clusterParameterTime: "
        "Timestamp(0, 0), boolData: false }, { _id: 'testIntClusterParameter', "
        "clusterParameterTime: Timestamp(0, 0), intData: 16 }, { _id: 'testStrClusterParameter', "
        "clusterParameterTime: Timestamp(0, 0), strData: 'off' } ] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogStartupOptionsOCSF) {
    // LogStartupOptions requires all the clusterParameterOptions, so we need
    // to create this decoration on the serviceContext. Since this happens in mongod/s
    // main, we need to replicate here.
    ChangeStreamOptionsManager::create(getServiceContext());

    logStartupOptions(getClient(), moe::Environment().toBSON());
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 1, class_uid: 1007, severity_id: 1, type_uid: 100701, "
        "actor: {}, type_id: 0, status_id: 1, unmapped: { atype: 'startup', startup_options: {}, "
        "cluster_parameters: [ { _id: 'addOrRemoveShardInProgress', clusterParameterTime: "
        "Timestamp(0, 0), inProgress: false }, { _id: 'changeStreamOptions', clusterParameterTime: "
        "Timestamp(0, 0), preAndPostImages: { expireAfterSeconds: 'off' } },{ _id: 'changeStreams',"
        "clusterParameterTime: Timestamp(0, 0), expireAfterSeconds: 3600 }, { _id: "
        "'configServerReadPreferenceForCatalogQueries', clusterParameterTime: Timestamp(0, 0), "
        "mustAlwaysUseNearest: false }, { _id: 'cwspTestNeedsFeatureFlagBlender', "
        "clusterParameterTime: Timestamp(0, 0), intData: 0 }, { _id: 'cwspTestNeedsLatestFCV', "
        "clusterParameterTime: Timestamp(0, 0), intData: 0 }, {_id: 'defaultMaxTimeMS', "
        "clusterParameterTime: Timestamp(0, 0), readOperations: 0}, { _id: 'fleCompactionOptions', "
        "clusterParameterTime: Timestamp(0, 0), maxCompactionSize: 268435456, "
        "maxAnchorCompactionSize: 268435456, maxESCEntriesPerCompactionDelete: 350000 }, { _id: "
        "'internalQueryCutoffForSampleFromRandomCursor', clusterParameterTime: Timestamp(0, 0), "
        "sampleCutoff: 0.05 }, "
        "{ '_id': 'internalSearchOptions', 'clusterParameterTime': { '$timestamp': { 't': 0,'i': 0 "
        "} }, 'oversubscriptionFactor': 1.064, 'batchSizeGrowthFactor' : 1.5 }, "
        "{ _id: 'pauseMigrationsDuringMultiUpdates', clusterParameterTime: "
        "Timestamp(0, 0), enabled: false }, {_id: 'querySettings', settingsArray: [], "
        "clusterParameterTime: Timestamp(0, 0)}, { _id: 'shardedClusterCardinalityForDirectConns', "
        "clusterParameterTime: Timestamp(0, 0), hasTwoOrMoreShards: false }, { _id: "
        "'testBoolClusterParameter', clusterParameterTime: Timestamp(0, 0), boolData: false }, { "
        "_id: 'testIntClusterParameter', clusterParameterTime: Timestamp(0, 0), intData: 16 }, { "
        "_id: 'testStrClusterParameter', clusterParameterTime: Timestamp(0, 0), strData: 'off' } ] "
        "} }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogShutdownMongo) {
    logShutdown(getClient());
    auto expectedOutputMongo =
        "{ atype: 'shutdown', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: {}, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogShutdownOCSF) {
    logShutdown(getClient());
    auto expectedOutputOCSF =
        "{ activity_id: 2, category_uid: 1, class_uid: 1007, severity_id: 1, type_uid: 100702, "
        "actor: {}, type_id: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogLogoutMongo) {
    logLogout(getClient(), "Kill the test!", kUsersBefore, kUsersAfter, boost::none);
    auto expectedOutputMongo =
        "{ atype: 'logout', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { reason: 'Kill the test!', initialUsers: [ { user: 'john', "
        "database: 'test' } ], updatedUsers: [] }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogLogoutOCSF) {
    logLogout(getClient(), "Kill the test!", kUsersBefore, kUsersAfter, boost::none);
    auto expectedOutputOCSF =
        "{ activity_id: 2, category_uid: 3, class_uid: 3002, severity_id: 1, type_uid: 300202, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, message: 'Reason: Kill the test!' }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

const auto kIndexSpec = BSON("id" << 1 << "name"
                                  << "test.id");

TEST_F(AuditMongoTest, basicLogCreateIndexMongo) {
    logCreateIndex(getClient(),
                   &kIndexSpec,
                   "test.id",
                   kNamespaceString,
                   "FAILED",
                   ErrorCodes::IndexAlreadyExists);
    auto expectedOutputMongo =
        "{ atype: 'createIndex', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test.coll', indexName: 'test.id', indexSpec: { id: 1, "
        "name: 'test.id' }, indexBuildState: 'FAILED' }, result: 68 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogCreateIndexOCSF) {
    logCreateIndex(getClient(),
                   &kIndexSpec,
                   "test.id",
                   kNamespaceString,
                   "FAILED",
                   ErrorCodes::IndexAlreadyExists);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300401, "
        "actor: {}, entity: { name: 'test.id@test.coll', type: 'create_index', data: { id: 1, "
        "name: 'test.id' } }, comment: 'IndexBuildState: FAILED' }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogCreateCollectionMongo) {
    logCreateCollection(getClient(), kNamespaceString);
    auto expectedOutputMongo =
        "{ atype: 'createCollection', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test.coll' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogCreateCollectionOCSF) {
    logCreateCollection(getClient(), kNamespaceString);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300401, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test.coll', type: "
        "'create_collection' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogCreateViewMongo) {
    logCreateView(getClient(),
                  kNamespaceStringAlt,
                  kNamespaceString,
                  BSONArray(),
                  ErrorCodes::InvalidViewDefinition);
    auto expectedOutputMongo =
        "{ atype: 'createCollection', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test.view', viewOn: 'test.coll', pipeline: [] }, "
        "result: 182 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogCreateViewOCSF) {
    logCreateView(getClient(),
                  kNamespaceStringAlt,
                  kNamespaceString,
                  BSONArray(),
                  ErrorCodes::InvalidViewDefinition);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300401, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test.coll', type: "
        "'collection' }, entity_result: { name: 'test.view', type: 'create_view', data: {} } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogImportCollectionMongo) {
    logImportCollection(getClient(), kNamespaceString);
    auto expectedOutputMongo =
        "{ atype: 'importCollection', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test.coll' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogImportCollectionOCSF) {
    logImportCollection(getClient(), kNamespaceString);
    auto expectedOutputOCSF =
        "{ activity_id: 3, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300403, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test.coll', type: "
        "'import_collection' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogCreateDatabaseMongo) {
    logCreateDatabase(getClient(), kDatabaseName);
    auto expectedOutputMongo =
        "{ atype: 'createDatabase', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogCreateDatabaseOCSF) {
    logCreateDatabase(getClient(), kDatabaseName);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300401, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test', type: "
        "'create_database' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogDropIndexMongo) {
    logDropIndex(getClient(), "test.id", kNamespaceString);
    auto expectedOutputMongo =
        "{ atype: 'dropIndex', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { ns: 'test.coll', indexName: 'test.id' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropIndexOCSF) {
    logDropIndex(getClient(), "test.id", kNamespaceString);
    auto expectedOutputOCSF =
        "{ activity_id: 4, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300404, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test.id@test.coll', type: "
        "'drop_index' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogDropCollectionMongo) {
    logDropCollection(getClient(), kNamespaceString);
    auto expectedOutputMongo =
        "{ atype: 'dropCollection', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test.coll' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropCollectionOCSF) {
    logDropCollection(getClient(), kNamespaceString);
    auto expectedOutputOCSF =
        "{ activity_id: 4, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300404, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test.coll', type: "
        "'drop_collection' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogDropViewMongo) {
    logDropView(getClient(),
                kNamespaceStringAlt,
                kNamespaceString,
                {},
                ErrorCodes::CommandNotSupportedOnView);
    auto expectedOutputMongo =
        "{ atype: 'dropCollection', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test.view', viewOn: 'test.coll', pipeline: [] }, "
        "result: 166 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropViewOCSF) {
    logDropView(getClient(),
                kNamespaceStringAlt,
                kNamespaceString,
                {},
                ErrorCodes::CommandNotSupportedOnView);
    auto expectedOutputOCSF =
        "{ activity_id: 4, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300404, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test.coll', type: "
        "'collection' }, entity_result: { name: 'test.view', type: 'drop_view' }, comment: { "
        "pipeline: [] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogDropDatabaseMongo) {
    logDropDatabase(getClient(), kDatabaseName);
    auto expectedOutputMongo =
        "{ atype: 'dropDatabase', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogDropDatabaseOCSF) {
    logDropDatabase(getClient(), kDatabaseName);
    auto expectedOutputOCSF =
        "{ activity_id: 4, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300404, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: { "
        "interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test', type: 'drop_database' "
        "} }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRenameCollectionMongo) {
    logRenameCollection(getClient(), kNamespaceStringAlt, kNamespaceString);
    auto expectedOutputMongo =
        "{ atype: 'renameCollection', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { old: 'test.view', new: 'test.coll' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogRenameCollectionOCSF) {
    logRenameCollection(getClient(), kNamespaceStringAlt, kNamespaceString);
    auto expectedOutputOCSF =
        "{ activity_id: 3, category_uid: 3, class_uid: 3004, severity_id: 1, type_uid: 300403, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: { "
        "interface_name: 'unix', ip: 'anonymous' }, entity: { name: 'test.view', type: "
        "'rename_collection_source' }, entity_result: { "
        "name: 'test.coll', type: 'rename_collection_destination' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogEnableShardingMongo) {
    logEnableSharding(getClient(), "test");
    auto expectedOutputMongo =
        "{ atype: 'enableSharding', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogEnableShardingOCSF) {
    logEnableSharding(getClient(), "test");
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 4, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'enableSharding', ns: 'test' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogAddShardMongo) {
    logAddShard(getClient(), "newShard", "rs1://localhost:27017,localhost:27018,localhost:27019");
    auto expectedOutputMongo =
        "{ atype: 'addShard', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], param: { shard: 'newShard', connectionString: "
        "'rs1://localhost:27017,localhost:27018,localhost:27019' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogAddShardOCSF) {
    logAddShard(getClient(), "newShard", "rs1://localhost:27017,localhost:27018,localhost:27019");
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 3, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'addShard', shard: 'newShard', connectionString: "
        "'rs1://localhost:27017,localhost:27018,localhost:27019' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRemoveShardMongo) {
    logRemoveShard(getClient(), "newShard");
    auto expectedOutputMongo =
        "{ atype: 'removeShard', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { shard: 'newShard' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogRemoveShardOCSF) {
    logRemoveShard(getClient(), "newShard");
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 4, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'removeShard', shard: 'newShard' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogShardCollectionMongo) {
    logShardCollection(getClient(),
                       kNamespaceString,
                       BSON("key"
                            << "DOB"),
                       true);
    auto expectedOutputMongo =
        "{ atype: 'shardCollection', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, "
        "users: [], roles: [], param: { ns: 'test.coll', key: { key: 'DOB' }, options: { unique: "
        "true } }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogShardCollectionOCSF) {
    logShardCollection(getClient(),
                       kNamespaceString,
                       BSON("key"
                            << "DOB"),
                       true);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 1, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'shardCollection', ns: 'test.coll', key: { key: 'DOB' "
        "}, options: { unique: true } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRefineCollectionShardKeyMongo) {
    logRefineCollectionShardKey(getClient(),
                                kNamespaceString,
                                BSON("key"
                                     << "age"));
    auto expectedOutputMongo =
        "{ atype: 'refineCollectionShardKey',local: { unix: 'anonymous' }, remote: { unix: "
        "'anonymous' }, users: [], roles: [], param: { ns: 'test.coll', key: { key: 'age' } }, "
        "result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogRefineCollectionShardKeyOCSF) {
    logRefineCollectionShardKey(getClient(),
                                kNamespaceString,
                                BSON("key"
                                     << "age"));
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 3, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'refineCollectionShardKey', ns: 'test.coll', key: { "
        "key: 'age' } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogInsertOperationMongo) {
    logInsertOperation(getClient(), kNamespaceStringPrivilege, BSON("foo" << 1));
    auto expectedOutputMongo =
        "{ atype: 'directAuthMutation',local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { document: { foo: 1 }, ns: 'admin.system.users', "
        "operation: 'insert' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogInsertOperationOCSF) {
    logInsertOperation(getClient(), kNamespaceStringPrivilege, BSON("foo" << 1));
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 3, class_uid: 3001, severity_id: 5, type_uid: 300101, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: "
        "{ interface_name: 'unix', ip: 'anonymous' }, unmapped: { atype: 'directAuthMutation', "
        "directAuthMutation: { namespace: 'admin.system.users', document: { foo: 1 } } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogUpdateOperationMongo) {
    logUpdateOperation(getClient(),
                       kNamespaceStringPrivilege,
                       BSON("old" << BSON("foo" << 1) << "new" << BSON("foo" << 2)));
    auto expectedOutputMongo =
        "{ atype: 'directAuthMutation', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { document: { old: { foo: 1 }, new: { foo: 2 } }, ns: "
        "'admin.system.users', operation: 'update' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogUpdateOperationOCSF) {
    logUpdateOperation(getClient(),
                       kNamespaceStringPrivilege,
                       BSON("old" << BSON("foo" << 1) << "new" << BSON("foo" << 2)));
    auto expectedOutputOCSF =
        "{ activity_id: 99, category_uid: 3, class_uid: 3001, severity_id: 5, type_uid: "
        "300199, actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, "
        "dst_endpoint: { interface_name: 'unix', ip: 'anonymous' }, unmapped: { atype: "
        "'directAuthMutation', directAuthMutation: { namespace: 'admin.system.users', document: { "
        "old: { foo: 1 }, new: { foo: 2 } } } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRemoveOperationMongo) {
    logRemoveOperation(getClient(), kNamespaceStringPrivilege, BSON("foo" << 2));
    auto expectedOutputMongo =
        "{ atype: 'directAuthMutation', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { document: { foo: 2 }, ns: 'admin.system.users', "
        "operation: 'remove' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogRemoveOperationOCSF) {
    logRemoveOperation(getClient(), kNamespaceStringPrivilege, BSON("foo" << 2));
    auto expectedOutputOCSF =
        "{ activity_id: 6, category_uid: 3, class_uid: 3001, severity_id: 5, type_uid: 300106, "
        "actor: {}, src_endpoint: { interface_name: 'unix', ip: 'anonymous' }, dst_endpoint: { "
        "interface_name: 'unix', ip: 'anonymous' }, unmapped: { atype: 'directAuthMutation', "
        "directAuthMutation: { namespace: "
        "'admin.system.users', document: { foo: 2 } } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogGetClusterParameterMongo) {
    logGetClusterParameter(getClient(), "replication.localPingThresholdMs");
    auto expectedOutputMongo =
        "{ atype: 'getClusterParameter', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { requestedClusterServerParameters: "
        "'replication.localPingThresholdMs' }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogGetClusterParameterOCSF) {
    logGetClusterParameter(getClient(), "replication.localPingThresholdMs");
    auto expectedOutputOCSF =
        "{ activity_id: 2, category_uid: 6, class_uid: 6003, severity_id: 1, type_uid: 600302, "
        "actor: {}, unmapped: { requestedClusterServerParameters: "
        "'replication.localPingThresholdMs' } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogSetClusterParameterMongo) {
    logSetClusterParameter(getClient(),
                           BSON("replication.localPingThresholdMs" << 1),
                           BSON("replication.localPingThresholdMs" << 100),
                           boost::none);
    auto expectedOutputMongo =
        "{ atype: 'setClusterParameter', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' "
        "}, users: [], roles: [], param: { originalClusterServerParameter: { "
        "'replication.localPingThresholdMs': 1 }, updatedClusterServerParameter: { "
        "'replication.localPingThresholdMs': 100 } }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogSetClusterParameterOCSF) {
    logSetClusterParameter(getClient(),
                           BSON("replication.localPingThresholdMs" << 1),
                           BSON("replication.localPingThresholdMs" << 100),
                           boost::none);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 1, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'setClusterParameter', originalClusterServerParameter: "
        "{ 'replication.localPingThresholdMs': 1 }, updatedClusterServerParameter: { "
        "'replication.localPingThresholdMs': 100 } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogUpdateCachedClusterParameterMongo) {
    logUpdateCachedClusterParameter(getClient(),
                                    BSON("security.sasl.hostName"
                                         << "localhost"),
                                    BSON("security.sasl.hostName"
                                         << "localhost2"),
                                    boost::none);
    auto expectedOutputMongo =
        "{ atype: 'updateCachedClusterServerParameter', local: { unix: 'anonymous' }, remote: { "
        "unix: 'anonymous' }, users: [], roles: [], param: { originalClusterServerParameter: { "
        "'security.sasl.hostName': 'localhost' }, updatedClusterServerParameter: { "
        "'security.sasl.hostName': 'localhost2' } }, result: 0 }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
}

TEST_F(AuditOCSFTest, basicLogUpdateCachedClusterParameterOCSF) {
    logUpdateCachedClusterParameter(getClient(),
                                    BSON("security.sasl.hostName"
                                         << "localhost"),
                                    BSON("security.sasl.hostName"
                                         << "localhost2"),
                                    boost::none);
    auto expectedOutputOCSF =
        "{ activity_id: 1, category_uid: 5, class_uid: 5002, severity_id: 1, type_uid: 500201, "
        "actor: {}, unmapped: { atype: 'updateCachedClusterServerParameter', "
        "originalClusterServerParameter: { 'security.sasl.hostName': 'localhost' }, "
        "updatedClusterServerParameter: { 'security.sasl.hostName': 'localhost2' } } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

TEST_F(AuditMongoTest, basicLogRotateLogMongo) {
    logRotateLog(getClient(), Status::OK(), {}, "_new.log");
    auto expectedOutputMongo =
        "{ atype: 'rotateLog', local: { unix: 'anonymous' }, remote: { unix: 'anonymous' }, users: "
        "[], roles: [], result: 0 }";
    auto param_field =
        "{logRotationStatus: { status: 'OK', rotatedLogPath: '_new.log', errors: [] } }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputMongo));
    ASSERT(log.hasField("param") && log.getField("param").isABSONObj());
    auto param_obj = log.getObjectField("param");
    checkPartialLogLine(param_obj, fromjson(param_field));
}

TEST_F(AuditOCSFTest, basicLogRotateLogOCSF) {
    logRotateLog(getClient(), Status::OK(), {}, "_new.log");
    auto expectedOutputOCSF =
        "{ activity_id: 99, category_uid: 1, class_uid: 1007, severity_id: 1, type_uid: "
        "100799, actor: {}, type_id: 0, status_id: 1, enrichments: [] }";
    BSONObj log = getLastNormalized();
    checkPartialLogLine(log, fromjson(expectedOutputOCSF));
}

}  // namespace

}  // namespace mongo::audit
