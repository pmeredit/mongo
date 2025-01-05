/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_client_attrs.h"
#include "audit/audit_manager.h"

#include "mongo/db/auth/authorization_session_test_fixture.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/service_context.h"
#include "mongo/util/options_parser/environment.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

namespace mongo::audit {

namespace {

class AuditDecorationsTest : public AuthorizationSessionTestFixture {
protected:
    explicit AuditDecorationsTest(Options options = Options{})
        : AuthorizationSessionTestFixture(std::move(options)) {
        auto* am = getGlobalAuditManager();
        if (am->isEnabled()) {
            LOGV2(9816000, "Auditing has already been enabled, skipping initialization");
            return;
        }

        moe::Environment env;
        ASSERT_OK(env.set(moe::Key("auditLog.destination"), moe::Value("mock")));
        getGlobalAuditManager()->initialize(env);

        // Validate initial state.
        ASSERT_EQ(am->isEnabled(), true);

        // Manually register the AuditClientObserver. There is no way to set auditLog.destination
        // before the unit test runs all global initializers, which means that auditing is initially
        // disabled. This causes the ServiceContext CAR responsible for registering the
        // AuditClientObserver to get skipped. So we do it here instead.
        getGlobalServiceContext()->registerClientObserver(std::make_unique<AuditClientObserver>());
    }
};

const UserName kUser1Test("user1"_sd, "test"_sd);
const std::unique_ptr<UserRequest> kUser1TestRequest =
    std::make_unique<UserRequestGeneral>(kUser1Test, boost::none);


TEST_F(AuditDecorationsTest, basicAuditUserAttrsCheck) {
    ASSERT_OK(createUser(kUser1Test, {}));

    auto newClient = getService()->makeClient("client1");
    ASSERT_OK(AuthorizationSession::get(newClient.get())
                  ->addAndAuthorizeUser(_opCtx.get(), kUser1TestRequest->clone(), boost::none));

    auto opCtx2 = newClient->makeOperationContext();
    auto auditAttrs = AuditUserAttrs::get(opCtx2.get());

    ASSERT(auditAttrs->username);
    ASSERT_EQ(auditAttrs->username->getUser(), "user1");
    ASSERT_EQ(auditAttrs->rolenames.size(), 0);
}

}  // namespace

}  // namespace mongo::audit
