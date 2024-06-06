/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <memory>

#include "mongo/unittest/unittest.h"

#include "ldap_host.h"
#include "ldap_runner.h"

namespace mongo {

/**
 * Mock implementation of the LDAPRunner. Consumers must load instantiations with
 * queries and their results using the ``push'' method, which stores them on a stack. runQuery will
 * pop the stack, assert its query matches the caller's, and return the stored result.
 */
class LDAPRunnerMock final : public LDAPRunner {
public:
    ~LDAPRunnerMock() final = default;
    Status bindAsUser(const std::string& user,
                      const SecureString& pwd,
                      TickSource* tickSource,
                      const SharedUserAcquisitionStats& userAcquisitionStats) final {
        return Status::OK();
    }

    Status checkLiveness(TickSource* tickSource,
                         const SharedUserAcquisitionStats& userAcquisitionStats) final {
        return Status::OK();
    }

    Status checkLivenessNotPooled(const LDAPConnectionOptions& connectionOptions,
                                  TickSource* tickSource,
                                  const SharedUserAcquisitionStats& userAcquisitionStats) final {
        return Status::OK();
    }


    StatusWith<LDAPEntityCollection> runQuery(
        const LDAPQuery& query,
        TickSource* tickSource,
        const SharedUserAcquisitionStats& userAcquisitionStats) final {
        UserAcquisitionStatsHandle handle(userAcquisitionStats.get(), tickSource, kSearch);
        ASSERT_FALSE(_stored.empty());
        auto next = std::move(_stored.back());
        _stored.pop_back();
        ASSERT_EQ(next.query, query);
        return next.results;
    };

    void push(std::string queryString, StatusWith<LDAPEntityCollection> entities) {
        auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfig(queryString);
        ASSERT_OK(swQueryParameters.getStatus());
        std::unique_ptr<LDAPQueryConfig> parameters =
            std::make_unique<LDAPQueryConfig>(std::move(swQueryParameters.getValue()));

        auto swLDAPQuery = LDAPQuery::instantiateQuery(*parameters, LDAPQueryContext::kUnitTest);
        ASSERT_OK(swLDAPQuery.getStatus());

        MockQueryEntry savedQuery{
            std::move(swLDAPQuery.getValue()), std::move(parameters), std::move(entities)};

        _stored.emplace_back(std::move(savedQuery));
    }

    std::vector<LDAPHost> getHosts() const final {
        return {};
    }

    bool hasHosts() const final {
        return false;
    }

    void setHosts(std::vector<LDAPHost> hosts) final {}

    Milliseconds getTimeout() const final {
        return Milliseconds(0);
    }
    void setTimeout(Milliseconds timeout) final {}

    int getRetryCount() const final {
        return 0;
    }

    void setRetryCount(int retryCount) final {}

    std::string getBindDN() const final {
        return "";
    }
    void setBindDN(const std::string& bindDN) final {}

    void setBindPasswords(std::vector<SecureString> pwd) final {}

    void setUseConnectionPool(bool) final {}

private:
    // The MockQueryEntry cannot be constructed piece by piece, because LDAPQuery doesn't have a
    // default constructor. But, LDAPQuery must be constructed with a reference to the final
    // LDAPQueryConfig object, after it has been moved or copied into its final resting place.
    // So, LDAPQueryConfig must be created, then moved onto the heap. LDAPQuery can be
    // constructed with a reference to the heap object and placed into the Entry.
    struct MockQueryEntry {
        LDAPQuery query;
        std::unique_ptr<LDAPQueryConfig> parameters;
        StatusWith<LDAPEntityCollection> results;
    };
    std::vector<MockQueryEntry> _stored;
};
}  // namespace mongo
