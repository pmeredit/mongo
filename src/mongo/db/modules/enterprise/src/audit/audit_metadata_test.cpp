/*
 *    Copyright (C) 2015 MongoDB Inc.
 */

#include "mongo/platform/basic.h"

#include <utility>

#include "mongo/db/jsobj.h"
#include "mongo/rpc/metadata/audit_metadata.h"
#include "mongo/unittest/unittest.h"

namespace {
using namespace mongo;
using namespace mongo::rpc;
using mongo::unittest::assertGet;

AuditMetadata checkParse(const BSONObj& metadata) {
    return assertGet(AuditMetadata::readFromMetadata(metadata));
}

TEST(AuditMetadata, ReadFromMetadata) {
    {
        // Empty object - should work just fine
        auto am = checkParse(BSONObj());
        ASSERT_FALSE(am.getImpersonatedUsersAndRoles());
    }
    {
        // empty impersonated users and roles
        auto am = checkParse(
            BSON("$audit" << BSON("$impersonatedUsers" << BSONArray() << "$impersonatedRoles"
                                                       << BSONArray())));

        // check users and roles have 0 length
        ASSERT_EQ(std::get<0>(*am.getImpersonatedUsersAndRoles()).size(), 0u);
        ASSERT_EQ(std::get<1>(*am.getImpersonatedUsersAndRoles()).size(), 0u);
    }
    {
        // empty impersonated users and roles, but reversed - we don't care about order.
        auto am = checkParse(
            BSON("$audit" << BSON("$impersonatedRoles" << BSONArray() << "$impersonatedUsers"
                                                       << BSONArray())));

        // check users and roles have 0 length
        ASSERT_EQ(std::get<0>(*am.getImpersonatedUsersAndRoles()).size(), 0u);
        ASSERT_EQ(std::get<1>(*am.getImpersonatedUsersAndRoles()).size(), 0u);
    }
    {
        // has 2 users but 0 roles - may be invalid w.r.t auditing system, but we
        // should parse this just fine.
        auto metadata =
            BSON("$audit" << BSON("$impersonatedUsers" << BSON_ARRAY(BSON("user"
                                                                          << "foo"
                                                                          << "db"
                                                                          << "bar")
                                                                     << BSON("user"
                                                                             << "baz"
                                                                             << "db"
                                                                             << "garply"))
                                                       << "$impersonatedRoles"
                                                       << BSONArray()));
        auto am = checkParse(metadata);
        auto users = std::get<0>(*am.getImpersonatedUsersAndRoles());

        ASSERT_EQ(users.size(), 2u);
        ASSERT_EQ(users[0].getUser(), "foo");
        ASSERT_EQ(users[0].getDB(), "bar");

        ASSERT_EQ(users[1].getUser(), "baz");
        ASSERT_EQ(users[1].getDB(), "garply");

        auto roles = std::get<1>(*am.getImpersonatedUsersAndRoles());

        ASSERT_EQ(roles.size(), 0u);
    }
    {
        // has roles but no users - may be invalid w.r.t. auditing system, but we should
        // parse this just fine.
        auto metadata =
            BSON("$audit" << BSON("$impersonatedUsers" << BSONArray() << "$impersonatedRoles"
                                                       << BSON_ARRAY(BSON("role"
                                                                          << "r0"
                                                                          << "db"
                                                                          << "db0")
                                                                     << BSON("role"
                                                                             << "r1"
                                                                             << "db"
                                                                             << "db1"))));
        auto am = checkParse(metadata);
        auto users = std::get<0>(*am.getImpersonatedUsersAndRoles());

        ASSERT_EQ(users.size(), 0u);

        auto roles = std::get<1>(*am.getImpersonatedUsersAndRoles());

        ASSERT_EQ(roles.size(), 2u);
        ASSERT_EQ(roles[0].getRole(), "r0");
        ASSERT_EQ(roles[0].getDB(), "db0");

        ASSERT_EQ(roles[1].getRole(), "r1");
        ASSERT_EQ(roles[1].getDB(), "db1");
    }
    {
        // has roles and users
        // has roles but no users - may be invalid w.r.t. auditing system, but we should
        // parse this just fine.
        auto metadata =
            BSON("$audit" << BSON("$impersonatedUsers" << BSON_ARRAY(BSON("user"
                                                                          << "u0"
                                                                          << "db"
                                                                          << "db0")
                                                                     << BSON("user"
                                                                             << "u1"
                                                                             << "db"
                                                                             << "db1"))
                                                       << "$impersonatedRoles"
                                                       << BSON_ARRAY(BSON("role"
                                                                          << "r0"
                                                                          << "db"
                                                                          << "db0")
                                                                     << BSON("role"
                                                                             << "r1"
                                                                             << "db"
                                                                             << "db1"))));
        auto am = checkParse(metadata);
        auto users = std::get<0>(*am.getImpersonatedUsersAndRoles());

        ASSERT_EQ(users.size(), 2u);
        ASSERT_EQ(users[0].getUser(), "u0");
        ASSERT_EQ(users[0].getDB(), "db0");

        ASSERT_EQ(users[1].getUser(), "u1");
        ASSERT_EQ(users[1].getDB(), "db1");

        auto roles = std::get<1>(*am.getImpersonatedUsersAndRoles());

        ASSERT_EQ(roles.size(), 2u);
        ASSERT_EQ(roles[0].getRole(), "r0");
        ASSERT_EQ(roles[0].getDB(), "db0");

        ASSERT_EQ(roles[1].getRole(), "r1");
        ASSERT_EQ(roles[1].getDB(), "db1");
    }
}

void checkParseFails(const BSONObj& metadata) {
    ASSERT_NOT_OK(AuditMetadata::readFromMetadata(metadata).getStatus());
}

TEST(AuditMetadata, ReadFromInvalidMetadata) {
    {
        // wrong type
        checkParseFails(BSON("$audit" << 3));
    }
    {
        // missing users
        checkParseFails(BSON("$audit" << BSON("$impersonatedRoles" << BSONArray())));
    }
    {
        // missing roles
        checkParseFails(BSON("$audit" << BSON("$impersonatedUsers" << BSONArray())));
    }
    {
        // users not array
        checkParseFails(BSON(
            "$audit" << BSON("$impersonatedUsers" << 3 << "$impersonatedRoles" << BSONArray())));
    }
    {
        // roles not array
        checkParseFails(BSON(
            "$audit" << BSON("$impersonatedUsers" << BSONArray() << "$impersonatedRoles" << 3)));
    }
    {
        // invalid users
        checkParseFails(
            BSON("$audit" << BSON("$impersonatedUsers" << BSON_ARRAY(2 << 3) << "$impersonatedRoles"
                                                       << BSONArray())));
    }
    {
        // invalid roles
        checkParseFails(
            BSON("$audit" << BSON("$impersonatedUsers" << BSONArray() << "$impersonatedRoles"
                                                       << BSON_ARRAY(1 << 2))));
    }
    {
        // extra keys
        checkParseFails(
            BSON("$audit" << BSON("$impersonatedUsers" << BSONArray() << "$impersonatedRoles"
                                                       << BSONArray()
                                                       << "thisshould"
                                                       << "notbehere")));
    }
}

}  // namespace
