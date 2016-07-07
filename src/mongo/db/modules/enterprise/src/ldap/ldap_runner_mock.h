/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

#include "ldap_runner.h"

namespace mongo {

/**
 * Mock implementation of the LDAPRunner. Consumers must load instantiations with
 * queries and their results using the ``push'' method, which stores them on a stack. runQuery will
 * pop the stack, assert its query matches the caller's, and return the stored result.
 */
class LDAPRunnerMock : public LDAPRunner {
public:
    ~LDAPRunnerMock() final = default;
    Status bindAsUser(const std::string& user, const SecureString& pwd) final {
        return Status::OK();
    }

    StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) final {
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
            stdx::make_unique<LDAPQueryConfig>(std::move(swQueryParameters.getValue()));

        auto swLDAPQuery = LDAPQuery::instantiateQuery(*parameters);
        ASSERT_OK(swLDAPQuery.getStatus());

        MockQueryEntry savedQuery{
            std::move(swLDAPQuery.getValue()), std::move(parameters), std::move(entities)};

        _stored.emplace_back(std::move(savedQuery));
    }

    std::vector<std::string> getHosts() const final {
        return {};
    }
    void setHosts(std::vector<std::string> hosts) final {}

    Milliseconds getTimeout() const final {
        return Milliseconds(0);
    }
    void setTimeout(Milliseconds timeout) final {}

    std::string getBindDN() const final {
        return "";
    }
    void setBindDN(const std::string& bindDN) final {}

    void setBindPassword(SecureString pwd) final {}

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
