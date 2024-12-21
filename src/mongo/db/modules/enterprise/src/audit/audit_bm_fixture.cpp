/**
 *    Copyright (C) 2024-present MongoDB, Inc.
 */

#include <fstream>

#include "audit_bm_fixture.h"

#include "mongo/db/auth/authorization_manager_factory_mock.h"
#include "mongo/unittest/assert.h"
#include "mongo/util/options_parser/environment.h"

namespace moe = mongo::optionenvironment;

namespace mongo::audit::bm {

template <typename F>
void warmup(F&& func, int iterations = 5) {
    for (int i = 0; i < iterations; i++) {
        std::invoke(std::forward<F>(func));
    }
}

template <typename T>
void AuditBenchmarkFixture<T>::initializeAudit(benchmark::State& state) {
    getGlobalAuditManager()->~AuditManager();
    new (getGlobalAuditManager()) AuditManager();

    auto getAuditDestinationString = [](AuditDestination dest) {
        switch (dest) {
            case AuditDestination::Syslog:
                return "syslog";
            case AuditDestination::Console:
                return "console";
            case AuditDestination::FileJSON:
            case AuditDestination::FileBSON:
                return "file";
            default:
                return "syslog";
        }
    };

    auto getAuditFileType = [](AuditDestination dest) {
        switch (dest) {
            case AuditDestination::FileJSON:
                return "JSON";
            case AuditDestination::FileBSON:
                return "BSON";
            default:
                return "JSON";
        }
    };

    moe::Environment env;
    if (auditDestination == AuditDestination::FileJSON ||
        auditDestination == AuditDestination::FileBSON) {
        // Create a unique temp file path
        auditLogPath = boost::filesystem::temp_directory_path() /
            boost::filesystem::unique_path("audit_%%%%_%%%%_%%%%_%%%%.log");

        // Create and verify the file
        std::ofstream outStream(auditLogPath.string());
        invariant(outStream.is_open());
        outStream.close();

        ASSERT_OK(env.set(moe::Key("auditLog.destination"), moe::Value("file")));
        ASSERT_OK(env.set(moe::Key("auditLog.path"), moe::Value(auditLogPath.string())));
        ASSERT_OK(
            env.set(moe::Key("auditLog.format"), moe::Value(getAuditFileType(auditDestination))));
    } else if (auditDestination != AuditDestination::None) {
        ASSERT_OK(env.set(moe::Key("auditLog.destination"),
                          moe::Value(getAuditDestinationString(auditDestination))));
    }

    if (auditFilter != AuditFilter::None) {
        ASSERT_OK(env.set(moe::Key("auditLog.filter"), moe::Value(auditFilterStr)));
    }

    getGlobalAuditManager()->initialize(env);

    makeServiceContext();
    AuditInterface::set(getServiceContext(), std::make_unique<T>());
}

template <typename T>
void AuditBenchmarkFixture<T>::SetUp(benchmark::State& state) {
    setBMArguments(state);
    initializeAudit(state);

    client = getServiceContext()->getService()->makeClient("benchmark_client");
}

template <typename T>
void AuditBenchmarkFixture<T>::TearDown(benchmark::State& state) {
    client.reset();
    AuditInterface::set(getServiceContext(), nullptr);
    setGlobalServiceContext(nullptr);
    serviceContextHolder = nullptr;

    // Remove created audit file if one exists.
    if ((auditDestination == AuditDestination::FileJSON ||
         auditDestination == AuditDestination::FileBSON) &&
        !auditLogPath.empty()) {
        try {
            boost::filesystem::remove(auditLogPath);
        } catch (const boost::filesystem::filesystem_error& e) {
            std::cerr << "Failed to remove audit log file: " << e.what() << std::endl;
        }
        auditLogPath.clear();
    }
}
template <typename T>
BSONObj AuditBenchmarkFixture<T>::serializeMetadataObject() {
    BSONObjBuilder builder;
    Status status = ClientMetadata::serialize(
        metadataValues.driver, metadataValues.version, metadataValues.appName, &builder);
    uassertStatusOK(status);

    return builder.obj();
}

template <typename T>
void AuditBenchmarkFixture<T>::setupClientMetadata() {
    BSONObj metadataObj = serializeMetadataObject();
    BSONElement metadataElem = metadataObj["client"];

    ClientMetadata::setFromMetadata(client.get(), metadataElem, true);
}


template <typename T>
void AuditBenchmarkFixture<T>::setupAuthorizationSession() {
    auto globalAuthzManagerFactory = std::make_unique<AuthorizationManagerFactoryMock>();
    AuthorizationManager::set(
        getServiceContext()->getService(),
        globalAuthzManagerFactory->createShard(getServiceContext()->getService()));

    std::shared_ptr<transport::Session> session = transportLayer.createSession();
    AuthorizationManager::get(getServiceContext()->getService())->setAuthEnabled(true);

    client = getServiceContext()->getService()->makeClient("testClient", session);
    authSession = AuthorizationSession::get(client.get());

    UserName username("usertest", "db");

    authSession->setImpersonatedUserData(UserName(metadataValues.driver, "db1"),
                                         {RoleName("sd", "sd")});
}

template <typename T>
void AuditBenchmarkFixture<T>::bmConstructClientMetadata(benchmark::State& state) {
    BSONObj metadataObj = serializeMetadataObject();
    BSONElement metadataElem = metadataObj["client"];

    warmup([&]() { ClientMetadata::setFromMetadata(client.get(), metadataElem, true); });

    // Benchmark
    for (auto _ : state) {
        ClientMetadata::setFromMetadata(client.get(), metadataElem, true);
    }

    setBMLabel(state);
}

template <typename T>
void AuditBenchmarkFixture<T>::bmLogClientMetadata(benchmark::State& state) {
    setupClientMetadata();

    warmup([&]() { logClientMetadata(client.get()); });

    // Benchmark
    for (auto _ : state) {
        logClientMetadata(client.get());
    }

    setBMLabel(state);
}

template <typename T>
void AuditBenchmarkFixture<T>::bmCreateBSONObjAudit(benchmark::State& state) {
    setupClientMetadata();

    // Basic serializer.
    auto serializer = [this](BSONObjBuilder* builder) {
        builder->append("driverName", metadataValues.driver);
        builder->append("driverVersion", metadataValues.version);
        builder->append("appName", metadataValues.appName);
    };

    auto createAuditEvent = [&]() {
        if constexpr (std::is_same_v<T, AuditMongo>) {
            return AuditMongo::AuditEventMongo(
                {client.get(), AuditEventType::kClientMetadata, serializer, ErrorCodes::OK});
        } else {
            return AuditOCSF::AuditEventOCSF({client.get(),
                                              ocsf::OCSFEventCategory::kNetworkActivity,
                                              ocsf::OCSFEventClass::kNetworkActivity,
                                              99,
                                              ocsf::kSeverityInformational,
                                              serializer,
                                              ErrorCodes::OK});
        }
    };

    warmup(createAuditEvent);

    // Benchmark
    for (auto _ : state) {
        createAuditEvent();
    }

    setBMLabel(state);
}

template <typename T>
void AuditBenchmarkFixture<T>::bmSetImpersonationClient(benchmark::State& state) {
    setupAuthorizationSession();

    audit::ImpersonatedClientAttrs impersonatedClientAttrs(client.get());

    warmup([&]() {
        authSession->setImpersonatedUserData(impersonatedClientAttrs.userName,
                                             impersonatedClientAttrs.roleNames);
    });

    // Benchmark
    for (auto _ : state) {
        authSession->setImpersonatedUserData(impersonatedClientAttrs.userName,
                                             impersonatedClientAttrs.roleNames);
    }

    setBMLabel(state);
}

template <typename T>
void AuditBenchmarkFixture<T>::bmLogImpersonatedClientMetadata(benchmark::State& state) {
    setupAuthorizationSession();
    setupClientMetadata();


    warmup([&]() { logClientMetadata(client.get()); });

    // Benchmark
    for (auto _ : state) {
        logClientMetadata(client.get());
    }

    setBMLabel(state);
}

template class AuditBenchmarkFixture<AuditMongo>;
template class AuditBenchmarkFixture<AuditOCSF>;
}  // namespace mongo::audit::bm
