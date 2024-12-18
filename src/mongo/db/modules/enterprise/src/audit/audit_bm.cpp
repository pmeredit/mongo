/**
 *    Copyright (C) 2024-present MongoDB, Inc.
 */

#include "audit_bm_fixture.h"

#include "audit/mongo/audit_mongo.h"
#include "audit/ocsf/audit_ocsf.h"

#include "mongo/logv2/log_domain_global.h"

namespace mongo::audit::bm {

MONGO_INITIALIZER_GENERAL(DisableLogging, (), ())
(InitializerContext*) {
    auto& lv2Manager = logv2::LogManager::global();
    logv2::LogDomainGlobal::ConfigurationOptions lv2Config;
    lv2Config.makeDisabled();
    uassertStatusOK(lv2Manager.getGlobalDomainInternal().configure(lv2Config));
}

const std::vector<int64_t> kSizes = {static_cast<int64_t>(BMSize::Small),
                                     static_cast<int64_t>(BMSize::Medium),
                                     static_cast<int64_t>(BMSize::Large)};

const std::vector<int64_t> kDestinations = {static_cast<int64_t>(AuditDestination::None),
                                            static_cast<int64_t>(AuditDestination::Syslog),
                                            static_cast<int64_t>(AuditDestination::Console),
                                            static_cast<int64_t>(AuditDestination::FileJSON),
                                            static_cast<int64_t>(AuditDestination::FileBSON)};

const std::vector<int64_t> kFilters = {static_cast<int64_t>(AuditFilter::None),
                                       static_cast<int64_t>(AuditFilter::Simple),
                                       static_cast<int64_t>(AuditFilter::Complex)};


// Benchmark creating client metadata audit event and logging it.
BENCHMARK_DEFINE_F(AuditMongoBMFixture, BM_ClientMetadataMongo)(benchmark::State& state) {
    bmLogClientMetadata(state);
}
BENCHMARK_DEFINE_F(AuditOCSFBMFixture, BM_ClientMetadataOCSF)(benchmark::State& state) {
    bmLogClientMetadata(state);
}

BENCHMARK_REGISTER_F(AuditMongoBMFixture, BM_ClientMetadataMongo)
    ->ArgsProduct({kSizes,
                   {static_cast<int64_t>(AuditDestination::None),
                    static_cast<int64_t>(AuditDestination::Syslog),
                    static_cast<int64_t>(AuditDestination::FileJSON),
                    static_cast<int64_t>(AuditDestination::FileBSON)},
                   kFilters});

BENCHMARK_REGISTER_F(AuditOCSFBMFixture, BM_ClientMetadataOCSF)
    ->ArgsProduct({kSizes,
                   {static_cast<int64_t>(AuditDestination::None),
                    static_cast<int64_t>(AuditDestination::Syslog),
                    static_cast<int64_t>(AuditDestination::FileJSON),
                    static_cast<int64_t>(AuditDestination::FileBSON)},
                   kFilters});


// Benchmark constucting client metadata from a BSONElement.
BENCHMARK_DEFINE_F(AuditMongoBMFixture, BM_ConstructClientMetadataMongo)(benchmark::State& state) {
    bmConstructClientMetadata(state);
}
BENCHMARK_DEFINE_F(AuditOCSFBMFixture, BM_ConstructClientMetadataOCSF)(benchmark::State& state) {
    bmConstructClientMetadata(state);
}

BENCHMARK_REGISTER_F(AuditMongoBMFixture, BM_ConstructClientMetadataMongo)
    ->ArgsProduct({kSizes, {0}, {0}});

BENCHMARK_REGISTER_F(AuditOCSFBMFixture, BM_ConstructClientMetadataOCSF)
    ->ArgsProduct({kSizes, {0}, {0}});


//  Benchmarks creating an audit event and storing it as a BSONObj.
BENCHMARK_DEFINE_F(AuditMongoBMFixture, BM_CreateBSONObjAuditMongo)(benchmark::State& state) {
    bmCreateBSONObjAudit(state);
}
BENCHMARK_DEFINE_F(AuditOCSFBMFixture, BM_CreateBSONObjAuditOCSF)(benchmark::State& state) {
    bmCreateBSONObjAudit(state);
}

BENCHMARK_REGISTER_F(AuditMongoBMFixture, BM_CreateBSONObjAuditMongo)
    ->ArgsProduct({kSizes, {0}, {0}});

BENCHMARK_REGISTER_F(AuditOCSFBMFixture, BM_CreateBSONObjAuditOCSF)
    ->ArgsProduct({kSizes, {0}, {0}});


//  Benchmarks setting impersonated user.
BENCHMARK_DEFINE_F(AuditMongoBMFixture, BM_SetImpersonationClientMongo)(benchmark::State& state) {
    bmSetImpersonationClient(state);
}
BENCHMARK_DEFINE_F(AuditOCSFBMFixture, BM_SetImpersonationClientOCSF)(benchmark::State& state) {
    bmSetImpersonationClient(state);
}

BENCHMARK_REGISTER_F(AuditMongoBMFixture, BM_SetImpersonationClientMongo)
    ->ArgsProduct({kSizes, {0}, {0}});

BENCHMARK_REGISTER_F(AuditOCSFBMFixture, BM_SetImpersonationClientOCSF)
    ->ArgsProduct({kSizes, {0}, {0}});

// Benchmark creating client metadata audit event with an impersonated user and logging it.
BENCHMARK_DEFINE_F(AuditMongoBMFixture, BM_ImpersonatedClientMetadataMongo)
(benchmark::State& state) {
    bmLogImpersonatedClientMetadata(state);
}
BENCHMARK_DEFINE_F(AuditOCSFBMFixture, BM_ImpersonatedClientMetadataOCSF)(benchmark::State& state) {
    bmLogImpersonatedClientMetadata(state);
}

BENCHMARK_REGISTER_F(AuditMongoBMFixture, BM_ImpersonatedClientMetadataMongo)
    ->ArgsProduct({kSizes,
                   {static_cast<int64_t>(AuditDestination::Syslog),
                    static_cast<int64_t>(AuditDestination::FileJSON),
                    static_cast<int64_t>(AuditDestination::FileBSON)},
                   kFilters});

BENCHMARK_REGISTER_F(AuditOCSFBMFixture, BM_ImpersonatedClientMetadataOCSF)
    ->ArgsProduct({kSizes,
                   {static_cast<int64_t>(AuditDestination::Syslog),
                    static_cast<int64_t>(AuditDestination::FileJSON),
                    static_cast<int64_t>(AuditDestination::FileBSON)},
                   kFilters});

}  // namespace mongo::audit::bm
