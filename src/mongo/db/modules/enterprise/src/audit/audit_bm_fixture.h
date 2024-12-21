/**
 *    Copyright (C) 2024-present MongoDB, Inc.
 */

#pragma once
#include <benchmark/benchmark.h>
#include <boost/filesystem.hpp>

#include "audit/audit_manager.h"
#include "audit/mongo/audit_mongo.h"
#include "audit/ocsf/audit_ocsf.h"

#include "mongo/db/audit.h"
#include "mongo/db/audit_interface.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/service_context.h"
#include "mongo/rpc/metadata/client_metadata.h"
#include "mongo/transport/mock_session.h"

namespace mongo::audit::bm {

enum class BMSize : int {
    Small = 0,
    Medium = 1,
    Large = 2,
};

enum class AuditDestination : int {
    None = 0,
    Syslog = 1,
    Console = 2,
    FileJSON = 3,
    FileBSON = 4,
};

enum class AuditFilter : int {
    None = 0,
    Simple = 1,
    Complex = 2,
};

/**
 * Fixture for benchmarking various Audit operations
 *
 * Requires three benchmark arguments to be provided:
 * 1. BMSize
 * 2. AuditDestination
 * 3. AuditFilter
 */
template <typename T>
class AuditBenchmarkFixture : public benchmark::Fixture {
public:
    struct MetadataValues {
        std::string driver;
        std::string version;
        std::string appName;
    };

protected:
    // Benchmark methods.
    void SetUp(benchmark::State& state) override;
    void TearDown(benchmark::State& state) override;

    // Setup Helper methods.
    void setupClientMetadata();
    void initializeAudit(benchmark::State& state);
    virtual void setBMArguments(benchmark::State& state);
    void makeServiceContext();
    ServiceContext* getServiceContext();
    BSONObj serializeMetadataObject();
    void setBMLabel(benchmark::State& state);
    void setupAuthorizationSession();

    // Benchmarks setting a metadata BSONElement into a Client.
    void bmConstructClientMetadata(benchmark::State& state);

    // Benchmarks logging the metadata of a client.
    void bmLogClientMetadata(benchmark::State& state);

    // Benchmarks creating an audit event and storing it as a BSONObj.
    void bmCreateBSONObjAudit(benchmark::State& state);

    // Benchmarks setting impersonation Client Metadata in an AuthorizationSession
    void bmSetImpersonationClient(benchmark::State& state);

    // Benchmarks logging the metadata of an impersonated client.
    void bmLogImpersonatedClientMetadata(benchmark::State& state);

    transport::TransportLayerMock transportLayer;
    ServiceContext::UniqueServiceContext serviceContextHolder;
    AuthorizationSession* authSession;
    ServiceContext::UniqueClient client;
    boost::filesystem::path auditLogPath;
    std::string auditFilterStr;

    // BM state env variables.
    BMSize bmSize;
    AuditDestination auditDestination;
    MetadataValues metadataValues;
    AuditFilter auditFilter;
};

template <typename T>
void AuditBenchmarkFixture<T>::setBMArguments(benchmark::State& state) {
    bmSize = static_cast<BMSize>(state.range(0));
    auditDestination = static_cast<AuditDestination>(state.range(1));
    auditFilter = static_cast<AuditFilter>(state.range(2));

    switch (bmSize) {
        case BMSize::Small:
            metadataValues = {"driverName", "driverVersion", ""};
            break;
        case BMSize::Medium:
            metadataValues = {"driverName_medium_size_for_bm",
                              "driverVersion_medium_size_for_bm",
                              "appName_medium_size_for_bm"};
            break;
        case BMSize::Large:
            metadataValues = {"driverName_large_size_for_bm_with_lots_of_extra_info_for_testing",
                              "driverVersion_large_size_for_bm_with_lots_of_extra_info_for_testing",
                              "appName_large_size_for_bm_with_lots_of_extra_info_for_testing"};
            break;
    }

    switch (auditFilter) {
        case AuditFilter::Simple:
            auditFilterStr = R"({ "atype": "clientMetadata" })";
            break;
        case AuditFilter::Complex:
            auditFilterStr = R"({
                                "$or": [
                                    {
                                        "$and": [
                                            { "param.clientMetadata.application.name": { "$regex": ".*appName.*" }},
                                            { "param.clientMetadata.driver.name": { "$regex": ".*driverName.*" }}
                                        ]
                                    },
                                    {
                                        "$and": [
                                            { "param.clientMetadata.application.name": { "$regex": ".*medium_size.*" }},
                                            { "param.clientMetadata.driver.name": { "$regex": ".*medium_size.*" }}
                                        ]
                                    },
                                    {
                                        "$and": [
                                            { "param.clientMetadata.application.name": { "$regex": ".*large_size.*" }},
                                            { "param.clientMetadata.driver.name": { "$regex": ".*large_size.*" }}
                                        ]
                                    }
                                ]
                            })";
            break;
        case AuditFilter::None:
            break;
    }
}

template <typename T>
void AuditBenchmarkFixture<T>::makeServiceContext() {
    serviceContextHolder = ServiceContext::make();
}


template <typename T>
ServiceContext* AuditBenchmarkFixture<T>::getServiceContext() {
    return serviceContextHolder.get();
}

template <typename T>
void AuditBenchmarkFixture<T>::setBMLabel(benchmark::State& state) {
    std::string sizeOutput;
    switch (bmSize) {
        case BMSize::Small:
            sizeOutput = "Small";
            break;
        case BMSize::Medium:
            sizeOutput = "Medium";
            break;
        case BMSize::Large:
            sizeOutput = "Large";
            break;
    }

    std::string destOutput;
    switch (auditDestination) {
        case AuditDestination::Syslog:
            destOutput = "Syslog";
            break;
        case AuditDestination::Console:
            destOutput = "Console";
            break;
        case AuditDestination::FileJSON:
            destOutput = "File JSON";
            break;
        case AuditDestination::FileBSON:
            destOutput = "File BSON";
            break;
        case AuditDestination::None:
            destOutput = "Audit Off";
            break;
    }

    std::string filter;
    switch (auditFilter) {
        case AuditFilter::None:
            filter = "No Filter";
            break;
        case AuditFilter::Simple:
            filter = "Simple Filter";
            break;
        case AuditFilter::Complex:
            filter = "Complex Filter";
            break;
    }

    std::string label = sizeOutput + " / " + destOutput + " / " + filter;
    const auto* metadata = ClientMetadata::getForClient(client.get());

    if (metadata) {
        state.counters["MetadataSize"] = metadata->getDocument().objsize();
    }
    state.SetLabel(label);
}

using AuditMongoBMFixture = AuditBenchmarkFixture<AuditMongo>;
using AuditOCSFBMFixture = AuditBenchmarkFixture<AuditOCSF>;

}  // namespace mongo::audit::bm
