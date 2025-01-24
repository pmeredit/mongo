/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "audit/audit_client_observer.h"
#include "audit/audit_manager.h"

#include "mongo/db/auth/authorization_session_test_fixture.h"
#include "mongo/db/auth/sasl_options.h"
#include "mongo/db/s/forwardable_operation_metadata.h"
#include "mongo/db/service_context.h"
#include "mongo/rpc/metadata/audit_client_attrs.h"
#include "mongo/rpc/metadata/audit_user_attrs.h"
#include "mongo/transport/asio/asio_tcp_fast_open.h"
#include "mongo/transport/asio/asio_transport_layer.h"
#include "mongo/transport/test_fixtures.h"
#include "mongo/util/options_parser/environment.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

namespace mongo::audit {

namespace {

void enableAuditing() {
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
}

class AuditUserAttrsTest : public AuthorizationSessionTestFixture {
protected:
    explicit AuditUserAttrsTest(Options options = makeOptions())
        : AuthorizationSessionTestFixture(std::move(options)) {
        enableAuditing();
    }

    static Options makeOptions() {
        return Options{}.addClientObserver(std::make_unique<AuditClientObserver>());
    }
};

class AuditAttrsCommunityTest : public AuthorizationSessionTestFixture {
protected:
    explicit AuditAttrsCommunityTest(Options options = Options{}) {
        auto* am = getGlobalAuditManager();
        ASSERT_FALSE(am->isEnabled());
        LOGV2(9791200, "Testing with disabled audit manager, skipping initialization");
    }
};

using namespace transport;

// Constants representing where the TransportLayer is actually listening.
constexpr auto kTestHostName = "127.0.0.1"_sd;
constexpr auto kTestMainPort = 22000;
constexpr auto kTestRouterPort = 22001;
constexpr auto kTestLoadBalancerPort = 22002;

// Constant representing a proxy protocol header.
constexpr auto kProxyProtocolHeader = "PROXY TCP4 10.122.9.63 54.225.237.121 1000 3000\r\n"_sd;

class AuditClientAttrsTestFixture : public ServiceContextTest {
protected:
    explicit AuditClientAttrsTestFixture()
        : _threadPool([]() {
              // Launch a separate background threadpool that can verify propagation of
              // ForwardableOperationMetadata.
              ThreadPool::Options options;
              options.poolName = "AuditClientAttrsTestFixture";
              options.minThreads = 0;
              options.maxThreads = 3;

              // Ensure all threads have a client.
              options.onCreateThread = [](const std::string& threadName) {
                  Client::initThread(
                      threadName,
                      getGlobalServiceContext()->getService(ClusterRole::ShardServer),
                      Client::noSession(),
                      ClientOperationKillableByStepdown{false});
              };

              return options;
          }()) {
        // Set up an AsioTransportLayer that binds to a system-assigned port on 127.0.0.1.
        ServerGlobalParams params;
        params.noUnixSocket = true;
        params.bind_ips = {kTestHostName.toString()};
        AsioTransportLayer::Options opts(&params);
        opts.port = kTestMainPort;
        opts.loadBalancerPort = kTestLoadBalancerPort;

        auto sessionManager = std::make_unique<test::MockSessionManager>();
        _sessionManager = sessionManager.get();
        _tla = std::make_unique<AsioTransportLayer>(std::move(opts), std::move(sessionManager));
        ASSERT_OK(_tla->setup());
        ASSERT_OK(_tla->start());

        enableAuditing();

        // Manually register the AuditClientObserver. There is no way to set auditLog.destination
        // before the unit test runs all global initializers, which means that auditing is initially
        // disabled. This causes the ServiceContext CAR responsible for registering the
        // AuditClientObserver to get skipped. So we do it here instead.
        getServiceContext()->registerClientObserver(std::make_unique<AuditClientObserver>());

        _threadPool.startup();
    }

    ~AuditClientAttrsTestFixture() override {
        _sessionManager->endAllSessions({});
        _tla->shutdown();
        _threadPool.shutdown();
        _threadPool.join();
    }

    void assertAuditClientAttrsMatch(const rpc::AuditClientAttrs& newAttrs,
                                     const rpc::AuditClientAttrs& oldAttrs) {
        ASSERT_EQ(newAttrs.getLocal().toString(), oldAttrs.getLocal().toString());
        ASSERT_EQ(newAttrs.getRemote().toString(), oldAttrs.getRemote().toString());
        ASSERT_EQ(newAttrs.getProxiedEndpoints(), oldAttrs.getProxiedEndpoints());
    }

protected:
    std::unique_ptr<AsioTransportLayer> _tla;
    test::MockSessionManager* _sessionManager;
    ThreadPool _threadPool;

private:
    std::shared_ptr<void> _disableTfo = tfo::setConfigForTest(0, 0, 0, 1024, Status::OK());
};

const UserName kUser1Test("user1"_sd, "test"_sd);
const std::unique_ptr<UserRequest> kUser1TestRequest =
    std::make_unique<UserRequestGeneral>(kUser1Test, boost::none);

TEST_F(AuditUserAttrsTest, basicAuditUserAttrsCheck) {
    ASSERT_OK(createUser(kUser1Test, {}));

    auto newClient = getService()->makeClient("client1");
    ASSERT_OK(AuthorizationSession::get(newClient.get())
                  ->addAndAuthorizeUser(_opCtx.get(), kUser1TestRequest->clone(), boost::none));

    auto opCtx2 = newClient->makeOperationContext();
    auto auditAttrs = rpc::AuditUserAttrs::get(opCtx2.get());

    ASSERT_EQ(auditAttrs->userName.getUser(), "user1");
    ASSERT_EQ(auditAttrs->roleNames.size(), 0);
}

TEST_F(AuditAttrsCommunityTest, basicAuditAttrsCommunityCheck) {
    ASSERT_OK(createUser(kUser1Test, {}));

    auto newClient = getService()->makeClient("client1");
    ASSERT_OK(AuthorizationSession::get(newClient.get())
                  ->addAndAuthorizeUser(_opCtx.get(), kUser1TestRequest->clone(), boost::none));

    auto opCtx2 = newClient->makeOperationContext();
    auto auditUserAttrs = rpc::AuditUserAttrs::get(opCtx2.get());
    auto auditClientAttrs = rpc::AuditClientAttrs::get(newClient.get());

    // community version does not have AuditManager and thus should not have observer setting up the
    // auditUserAttrs or auditClientAttrs decorations
    ASSERT_FALSE(auditUserAttrs);
    ASSERT_FALSE(auditClientAttrs);
}

TEST_F(AuditClientAttrsTestFixture, directAuditClientAttrs) {
    // Simulate a direct connection to the AsioTransportLayer and assert that newly-created clients
    // see Session information propagated over to the Client's AuditClientAttrs.
    auto onStartSession = std::make_shared<Notification<void>>();
    _sessionManager->setOnStartSession([&](test::SessionThread& st) {
        // Check that the session contains the expected values.
        ASSERT_EQ(st.session()->local().host(), kTestHostName);
        ASSERT_EQ(st.session()->local().port(), kTestMainPort);
        ASSERT_TRUE(st.session()->local().isLocalHost());
        ASSERT_FALSE(st.session()->isFromRouterPort());
        ASSERT_FALSE(st.session()->isFromLoadBalancer());
        ASSERT_FALSE(st.session()->getProxiedDstEndpoint());

        // Check that auditClientAttrs exists on the newly-created client and matches the values on
        // the transport session.
        auto client =
            getServiceContext()->getService()->makeClient("AuditClientAttrsTest", st.session());
        auto opCtx = client->makeOperationContext();
        ASSERT_FALSE(client->isRouterClient());
        auto auditClientAttrs = rpc::AuditClientAttrs::get(client.get());
        ASSERT_TRUE(auditClientAttrs);
        ASSERT_EQ(auditClientAttrs->getLocal().toString(), st.session()->local().toString());
        ASSERT_EQ(auditClientAttrs->getRemote().toString(), st.session()->remote().toString());
        ASSERT_EQ(auditClientAttrs->getRemote().toString(),
                  st.session()->getSourceRemoteEndpoint().toString());
        ASSERT_EQ(auditClientAttrs->getProxiedEndpoints(), std::vector<HostAndPort>{});

        // Propagate AuditClientAttrs to another thread via ForwardableOperationMetadata.
        auto onBackgroundThreadComplete = std::make_shared<Notification<void>>();
        ForwardableOperationMetadata opMetadata(opCtx.get());
        _threadPool.schedule([this, &opMetadata, &onBackgroundThreadComplete, auditClientAttrs](
                                 Status schedStatus) mutable noexcept {
            auto* backgroundClient = Client::getCurrent();
            auto backgroundOpCtx = backgroundClient->makeOperationContext();

            // Since this client was created in the background without a transport::Session, it is
            // not expected to have an AuditClientAttrs.
            ASSERT_FALSE(rpc::AuditClientAttrs::get(backgroundClient));

            // Set the old OperationContext/Client's info onto this one, including
            // AuditClientAttrs.
            opMetadata.setOn(backgroundOpCtx.get());

            // Now, this thread's client should also have the same AuditClientAttrs as the
            // parent.
            auto backgroundAuditClientAttrs = rpc::AuditClientAttrs::get(backgroundClient);
            ASSERT_TRUE(backgroundAuditClientAttrs);
            assertAuditClientAttrsMatch(backgroundAuditClientAttrs.value(),
                                        auditClientAttrs.value());
            onBackgroundThreadComplete->set();
        });

        // Wait for the background thread to signal that it has finished its assertions before
        // signaling to the outermost client thread that session establishment is complete.
        onBackgroundThreadComplete->get();
        onStartSession->set();
    });

    // Connect to the main port that _tla is listening on.
    auto swSession = _tla->connect(
        {kTestHostName.toString(), kTestMainPort}, ConnectSSLMode::kDisableSSL, Seconds{10}, {});
    ASSERT_OK(swSession);

    onStartSession->get();
}

TEST_F(AuditClientAttrsTestFixture, loadBalancedAuditClientAttrs) {
    // Simulate a load-balanced connection to the AsioTransportLayer and assert that
    // newly-created clients see Session information propagated over to the Client's
    // AuditClientAttrs.
    auto onStartSession = std::make_shared<Notification<void>>();
    _sessionManager->setOnStartSession([&](test::SessionThread& st) {
        // Check that the session contains the expected values.
        ASSERT_EQ(st.session()->local().host(), kTestHostName);
        ASSERT_EQ(st.session()->local().port(), kTestLoadBalancerPort);
        ASSERT_TRUE(st.session()->local().isLocalHost());
        ASSERT_FALSE(st.session()->isFromRouterPort());
        ASSERT_TRUE(st.session()->isFromLoadBalancer());
        ASSERT_TRUE(st.session()->getProxiedDstEndpoint());

        // Check that auditClientAttrs exists on the newly-created client and matches the values
        // on the transport session.
        auto client =
            getServiceContext()->getService()->makeClient("AuditClientAttrsTest", st.session());
        auto opCtx = client->makeOperationContext();
        ASSERT_FALSE(client->isRouterClient());
        auto auditClientAttrs = rpc::AuditClientAttrs::get(client.get());
        ASSERT_TRUE(auditClientAttrs);
        ASSERT_EQ(auditClientAttrs->getLocal().toString(), st.session()->local().toString());
        ASSERT_NE(auditClientAttrs->getRemote().toString(), st.session()->remote().toString());
        ASSERT_EQ(auditClientAttrs->getRemote().toString(),
                  st.session()->getSourceRemoteEndpoint().toString());
        ASSERT_EQ(auditClientAttrs->getProxiedEndpoints(),
                  std::vector<HostAndPort>{st.session()->getProxiedDstEndpoint().value()});

        // Propagate AuditClientAttrs to another thread via ForwardableOperationMetadata.
        auto onBackgroundThreadComplete = std::make_shared<Notification<void>>();
        ForwardableOperationMetadata opMetadata(opCtx.get());
        _threadPool.schedule([this, &opMetadata, &onBackgroundThreadComplete, auditClientAttrs](
                                 Status schedStatus) mutable noexcept {
            auto* backgroundClient = Client::getCurrent();
            auto backgroundOpCtx = backgroundClient->makeOperationContext();

            // Since this client was created in the background without a transport::Session, it
            // is not expected to have an AuditClientAttrs.
            ASSERT_FALSE(rpc::AuditClientAttrs::get(backgroundClient));

            // Set the old OperationContext/Client's info onto this one, including
            // AuditClientAttrs.
            opMetadata.setOn(backgroundOpCtx.get());

            // Now, this thread's client should also have the same AuditClientAttrs as the
            // parent.
            auto backgroundAuditClientAttrs = rpc::AuditClientAttrs::get(backgroundClient);
            ASSERT_TRUE(backgroundAuditClientAttrs);
            assertAuditClientAttrsMatch(backgroundAuditClientAttrs.value(),
                                        auditClientAttrs.value());
            onBackgroundThreadComplete->set();
        });

        // Wait for the background thread to signal that it has finished its assertions before
        // signaling to the outermost client thread that session establishment is complete.
        onBackgroundThreadComplete->get();

        onStartSession->set();
    });

    // Connect to the load balancer port that _tla is listening on and then write a proxy
    // protocol header.
    asio::io_context ctx{};
    asio::ip::tcp::socket sock{ctx};
    std::error_code ec;
    ec = sock.connect(asio::ip::tcp::endpoint(asio::ip::make_address(kTestHostName.toString()),
                                              kTestLoadBalancerPort),
                      ec);
    ASSERT_FALSE(ec) << errorMessage(ec);

    asio::write(sock, asio::buffer(kProxyProtocolHeader.data(), kProxyProtocolHeader.size()), ec);
    ASSERT_FALSE(ec) << errorMessage(ec);

    onStartSession->get();
}

}  // namespace

}  // namespace mongo::audit
