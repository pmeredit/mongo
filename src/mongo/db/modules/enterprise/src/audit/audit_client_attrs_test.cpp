/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_client_attrs.h"

#include "mongo/crypto/mechanism_scram.h"
#include "mongo/crypto/sha1_block.h"
#include "mongo/db/auth/authorization_backend_mock.h"
#include "mongo/db/auth/authorization_session_test_fixture.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/transport/transport_layer_mock.h"
#include "mongo/unittest/unittest.h"

namespace mongo::audit {

namespace {

using AuditClientAttrsTest = AuthorizationSessionTestFixture;

const UserName kUser1Test("user1"_sd, "test"_sd);
const std::unique_ptr<UserRequest> kUser1TestRequest =
    std::make_unique<UserRequestGeneral>(kUser1Test, boost::none);


TEST_F(AuditClientAttrsTest, basicAuditAttrsCheck) {
    ASSERT_OK(createUser(kUser1Test, {}));

    auto newClient = getService()->makeClient("client1");
    ASSERT_OK(AuthorizationSession::get(newClient.get())
                  ->addAndAuthorizeUser(_opCtx.get(), kUser1TestRequest->clone(), boost::none));

    auto opCtx2 = newClient->makeOperationContext();
    auto auditAttrs = AuditClientAttrs::get(opCtx2.get());
    ASSERT(auditAttrs->username);
    ASSERT_EQ(auditAttrs->username->getUser(), "user1");
    ASSERT_EQ(auditAttrs->rolenames.size(), 0);
}

}  // namespace

}  // namespace mongo::audit
