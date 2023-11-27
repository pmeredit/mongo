/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "document_source_list_search_indexes.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/oid.h"
#include "mongo/db/auth/authorization_checks.h"
#include "mongo/db/auth/authorization_session_test_fixture.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/auth/resource_pattern.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/tenant_id.h"
#include "mongo/util/assert_util.h"

namespace mongo {

using ListSearchIndexesAuthTest = AuthorizationSessionTestFixture;

namespace {
const TenantId kTenantId1(OID("12345678901234567890aaaa"));
const NamespaceString nss =
    NamespaceString::createNamespaceString_forTest(kTenantId1, "test", "foo");

TEST_F(ListSearchIndexesAuthTest, CanAggregateListSearchIndexesWithSearchIndexesAction) {
    auto rsrc = ResourcePattern::forDatabaseName(nss.dbName());

    authzSession->assumePrivilegesForDB(Privilege(rsrc, ActionType::listSearchIndexes),
                                        nss.dbName());

    BSONArray pipeline = BSON_ARRAY(BSON("$listSearchIndexes" << BSONObj()));
    auto aggReq = buildAggReq(nss, pipeline);
    PrivilegeVector privileges =
        uassertStatusOK(auth::getPrivilegesForAggregate(authzSession.get(), nss, aggReq, false));
    ASSERT_TRUE(authzSession->isAuthorizedForPrivileges(privileges));
}

TEST_F(ListSearchIndexesAuthTest, CannotAggregateListSearchIndexesWithoutSearchIndexesAction) {
    auto rsrc = ResourcePattern::forDatabaseName(nss.dbName());

    authzSession->assumePrivilegesForDB(Privilege(rsrc, ActionType::find), nss.dbName());

    BSONArray pipeline = BSON_ARRAY(BSON("$listSearchIndexes" << BSONObj()));
    auto aggReq = buildAggReq(nss, pipeline);
    PrivilegeVector privileges =
        uassertStatusOK(auth::getPrivilegesForAggregate(authzSession.get(), nss, aggReq, false));
    ASSERT_FALSE(authzSession->isAuthorizedForPrivileges(privileges));
}

}  // namespace
}  // namespace mongo
